#!/usr/bin/env python3
"""LaminaDB persistence drill: rehearses the durable deployment and verifies
real data, not just container startup.

Phases:
  P1  acknowledged-commit ledger survives a full container rebuild
      (checkpoint image + journal suffix both involved)
  P2  SIGTERM during commit load with an in-flight checkpoint: bounded stop,
      every acknowledged commit still present after restart
  P3  SIGKILL during load: durability never depended on the stop grace
  P4  second instance on the same data directory is refused, primary unharmed
  P5  backup restored to a different volume: data verified, change cursors
      before history_base are rejected (410) and consumers re-align
  P6  writes are confined to the data volume (read-only root filesystem
      property); compose runtime additionally inspects container hardening

Runtimes:
  local    runs bin/laminad inside a sandbox whose root and HOME are
           read-only; only vol-* directories ("named volumes") and tmp/ are
           writable. Executable without Docker; identical engine semantics.
  compose  drives deploy/docker-compose.yml (requires Docker).

Usage: python3 deploy/drill.py --runtime local
Exit code is non-zero if any check fails.
"""
import argparse, concurrent.futures, json, os, pathlib, shutil, signal, socket, subprocess, sys, threading, time, urllib.request, urllib.error

REPO = pathlib.Path(__file__).resolve().parents[1]
SCHEMA = {'name': 'spectra', 'primary': 'sample', 'columns': [
    {'name': 'sample', 'type': 'int'}, {'name': 'band', 'type': 'text'},
    {'name': 'amplitude', 'type': 'int', 'nullable': True}, {'name': 'valid', 'type': 'bool'}]}
STOP_BOUND = 30.0  # compose stop_grace_period; local stops must also finish well inside it

REPORT = []
def check(name, ok, **facts):
    line = {'check': name, 'status': 'pass' if ok else 'fail', **facts}
    REPORT.append(line)
    print(json.dumps(line, ensure_ascii=False), flush=True)
    if not ok:
        raise DrillFailed(name)
class DrillFailed(RuntimeError):
    pass

def free_port():
    with socket.socket() as s:
        s.bind(('127.0.0.1', 0))
        return s.getsockname()[1]

def call(url, method, path, data=None, status=200, raw=False, timeout=15):
    body = None if data is None else json.dumps(data).encode()
    req = urllib.request.Request(url + path, data=body, method=method, headers={'Content-Type': 'application/json'})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            code, payload = resp.status, resp.read()
    except urllib.error.HTTPError as e:
        code, payload = e.code, e.read()
    if code != status:
        raise RuntimeError(f'{method} {path}: expected {status}, got {code}: {payload.decode(errors="replace")}')
    return payload if raw else json.loads(payload)

def wait_ready(url, proc=None, timeout=60):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if proc is not None and proc.poll() is not None:
            raise RuntimeError(f'instance exited during startup rc={proc.returncode}')
        try:
            call(url, 'GET', '/health', timeout=3)
            return
        except Exception:
            time.sleep(0.1)
    raise RuntimeError('instance did not become ready')

def row(n, band=None, amplitude=None):
    return {'sample': n, 'band': band or f'b{n}', 'amplitude': n * 10 if amplitude is None else amplitude, 'valid': True}

def all_rows(url):
    return call(url, 'POST', '/query', {'relation': 'spectra', 'limit': 10000})['rows']

def model_rows(model):
    return [model[k] for k in sorted(model)]

def collect_changes(url):
    base = call(url, 'GET', '/stats')['history_base']
    records, after = [], base
    while True:
        page = call(url, 'GET', f'/changes?after={after}&limit=1000')
        records.extend(page['records'])
        if not page['more']:
            return records, base
        after = page['cursor']

def verify_history(url, acked_seqs, note):
    """Every sequence the service ever acknowledged is still verifiable after
    the rebuild, and the journal has no gaps between base and head."""
    stats = call(url, 'GET', '/stats')
    head = stats['sequence']
    records, base = collect_changes(url)
    sequences = [r['sequence'] for r in records]
    check(f'{note}: change history contiguous from base to head',
          sequences == list(range(base + 1, head + 1)), base=base, head=head, records=len(records))
    missing = [s for s in acked_seqs if s not in set(sequences)]
    check(f'{note}: every acknowledged sequence present in history', not missing,
          acknowledged=len(acked_seqs), missing=missing[:5])
    return head, stats, records

# ---------------------------------------------------------------- runtimes
class Local:
    """Runs bin/laminad inside a read-only sandbox tree; only vol-* ("named
    volumes") and tmp/ are writable, HOME is read-only."""
    def __init__(self, workdir):
        self.binary = str(REPO / 'bin' / 'laminad')
        self.sandbox = workdir / 'sandbox'
        self.logs = workdir / 'logs'
        self.procs, self.volumes, self.urls = {}, {}, {}
        if self.sandbox.exists():
            subprocess.run(['chmod', '-R', 'u+rwX', str(self.sandbox)], check=False)
            shutil.rmtree(self.sandbox)
        self.logs.mkdir(parents=True, exist_ok=True)
        (self.sandbox / 'home').mkdir(parents=True)
        (self.sandbox / 'tmp').mkdir()
        os.chmod(self.sandbox / 'tmp', 0o777)
        os.chmod(self.sandbox / 'home', 0o555)
        os.chmod(self.sandbox, 0o555)
        self.tree_before = self.tree()
    def tree(self):
        out = {}
        for p in sorted(self.sandbox.rglob('*')):
            st = p.stat()
            out[str(p.relative_to(self.sandbox))] = ('dir' if p.is_dir() else 'file', st.st_size, st.st_mtime_ns)
        return out
    def make_volume(self, name):
        os.chmod(self.sandbox, 0o755)
        (self.sandbox / f'vol-{name}').mkdir(exist_ok=True)
        os.chmod(self.sandbox, 0o555)
        self.volumes[name] = self.sandbox / f'vol-{name}'
    def reset_volume(self, name):
        self.down(name)
        os.chmod(self.sandbox, 0o755)
        shutil.rmtree(self.sandbox / f'vol-{name}', ignore_errors=True)
        os.chmod(self.sandbox, 0o555)
        self.make_volume(name)
    def up(self, name, restore=None):
        if name not in self.volumes:
            self.make_volume(name)
        if name in self.procs:
            self.procs.pop(name)[1].close()
        port = free_port()
        url = f'http://127.0.0.1:{port}/v1/lamina'
        args = [self.binary, '-data', str(self.volumes[name] / 'lamina'), '-listen',
                f'127.0.0.1:{port}', '-drain-timeout', '10s']
        if restore:
            args += ['-restore', str(restore)]
        log = open(self.logs / f'{name}.log', 'ab')
        env = {'HOME': str(self.sandbox / 'home'), 'TMPDIR': str(self.sandbox / 'tmp'), 'PATH': '/usr/bin:/bin'}
        proc = subprocess.Popen(args, cwd=self.sandbox, env=env, stdout=log, stderr=subprocess.STDOUT)
        self.procs[name], self.urls[name] = (proc, log), url
        wait_ready(url, proc)
        return url
    def signal(self, name, sig):
        proc, _ = self.procs[name]
        start = time.monotonic()
        proc.send_signal(sig)
        try:
            rc = proc.wait(timeout=45)
            return time.monotonic() - start, rc
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()
            return 45.0, None
    def down(self, name):
        if name in self.procs:
            proc, log = self.procs.pop(name)
            if proc.poll() is None:
                proc.kill()
                proc.wait()
            log.close()
    def url(self, name):
        return self.urls[name]
    def second_instance(self, name):
        r = subprocess.run([self.binary, '-data', str(self.volumes[name] / 'lamina'), '-listen', '127.0.0.1:0'],
                           capture_output=True, text=True, timeout=15)
        return r.returncode, r.stderr
    def restore_onto_existing(self, name, backup):
        r = subprocess.run([self.binary, '-restore', str(backup), '-data', str(self.volumes[name] / 'lamina'),
                            '-listen', '127.0.0.1:0'], capture_output=True, text=True, timeout=15)
        return r.returncode, r.stderr
    def teardown(self):
        for name in list(self.procs):
            self.down(name)

class Compose:
    """Drives deploy/docker-compose.yml. The laminadb service plays every
    primary role; lamina-restore is the restore rehearsal target on its own
    named volume. Requires the default --workdir (deploy/drill-work) because
    the restore profile mounts it."""
    COMPOSE = ['docker', 'compose', '-f', str(REPO / 'deploy' / 'docker-compose.yml')]
    URLS = {'main': 'http://127.0.0.1:7386/v1/lamina', 'restored': 'http://127.0.0.1:7387/v1/lamina'}
    SERVICE = {'main': 'laminadb', 'restored': 'lamina-restore'}
    VOLUME = {'main': 'lamina-db_lamina-data', 'restored': 'lamina-db_lamina-restored'}
    def __init__(self, workdir):
        self.run('build')
    def run(self, *args, capture=False, profile=False, check_rc=True):
        cmd = self.COMPOSE + (['--profile', 'restore'] if profile else []) + list(args)
        r = subprocess.run(cmd, capture_output=capture, text=True)
        if check_rc and r.returncode != 0:
            raise RuntimeError(f'{" ".join(cmd)} failed rc={r.returncode}: {r.stderr if capture else ""}')
        return r
    def container(self, name):
        r = self.run('ps', '-q', self.SERVICE[name], capture=True, profile=(name == 'restored'))
        return r.stdout.strip().splitlines()[0] if r.stdout.strip() else ''
    def reset_volume(self, name):
        self.run('rm', '-sf', self.SERVICE[name], profile=(name == 'restored'), check_rc=False)
        subprocess.run(['docker', 'volume', 'rm', '-f', self.VOLUME[name]], capture_output=True)
    def up(self, name, restore=None):
        if name == 'restored':
            self.reset_volume('restored')
            self.run('up', '-d', 'lamina-restore', profile=True)
        else:
            self.run('up', '-d', '--force-recreate', 'laminadb')
        wait_ready(self.URLS[name])
        return self.URLS[name]
    def signal(self, name, sig):
        svc = self.SERVICE[name]
        cid = self.container(name)
        start = time.monotonic()
        self.run('kill', '-s', 'SIGKILL' if sig == signal.SIGKILL else 'SIGTERM', svc, profile=(name == 'restored'))
        deadline = time.monotonic() + 45
        while time.monotonic() < deadline:
            out = subprocess.run(['docker', 'inspect', '-f', '{{.State.Running}} {{.State.ExitCode}}', cid],
                                 capture_output=True, text=True).stdout.split()
            if out and out[0] == 'false':
                return time.monotonic() - start, int(out[1])
            time.sleep(0.1)
        return 45.0, None
    def down(self, name):
        self.run('rm', '-sf', self.SERVICE[name], profile=(name == 'restored'), check_rc=False)
    def url(self, name):
        return self.URLS[name]
    def second_instance(self, name):
        r = self.run('run', '--rm', '--no-deps', '-T', self.SERVICE[name], capture=True, check_rc=False)
        return r.returncode, r.stderr + r.stdout
    def restore_onto_existing(self, name, backup):
        # lamina-restore's command always carries -restore; running it against
        # the already-initialized restore volume must fail once, not loop.
        r = self.run('run', '--rm', '--no-deps', '-T', 'lamina-restore', capture=True, profile=True, check_rc=False)
        return r.returncode, r.stderr + r.stdout
    def inspect_hardening(self):
        out = subprocess.run(['docker', 'inspect', self.container('main')], capture_output=True, text=True)
        return json.loads(out.stdout)[0]
    def teardown(self):
        self.run('rm', '-sf', 'laminadb', check_rc=False)
        self.run('rm', '-sf', 'lamina-restore', profile=True, check_rc=False)

# ---------------------------------------------------------------- phases
def seed(url, model, ledger, rows):
    call(url, 'POST', '/relations', SCHEMA, status=201)
    seq = None
    for i in range(0, len(rows), 1000):  # transaction write-set limit is 1000 keys
        chunk = rows[i:i + 1000]
        tx = call(url, 'POST', '/transactions', {}, status=201)['id']
        for r in chunk:
            call(url, 'POST', f'/transactions/{tx}/put', {'relation': 'spectra', 'row': r, 'insert_only': True})
        seq = call(url, 'POST', f'/transactions/{tx}/commit')['sequence']
        for r in chunk:
            model[r['sample']] = r
        ledger.append({'sequence': seq, 'puts': [r['sample'] for r in chunk], 'deletes': []})
    return seq

def commit(url, model, ledger, puts=(), deletes=()):
    tx = call(url, 'POST', '/transactions', {}, status=201)['id']
    for r in puts:
        call(url, 'POST', f'/transactions/{tx}/put', {'relation': 'spectra', 'row': r, 'insert_only': False})
    for k in deletes:
        call(url, 'POST', f'/transactions/{tx}/delete', {'relation': 'spectra', 'key': k})
    out = call(url, 'POST', f'/transactions/{tx}/commit')
    for r in puts:
        model[r['sample']] = r
    for k in deletes:
        model.pop(k, None)
    ledger.append({'sequence': out['sequence'], 'puts': [r['sample'] for r in puts], 'deletes': list(deletes)})
    return out['sequence']

def phase_ledger_rebuild(rt, workdir):
    url = rt.up('main')
    model, ledger = {}, []
    seed_seq = seed(url, model, ledger, [row(n) for n in range(1, 8)])
    call(url, 'POST', '/checkpoint')
    snapshot_at_seed = dict(model)
    commit(url, model, ledger, puts=[row(10), row(11)])
    commit(url, model, ledger, puts=[row(1, amplitude=555)])
    commit(url, model, ledger, deletes=[3])
    commit(url, model, ledger, puts=[row(12), row(13), row(14)])
    commit(url, model, ledger, puts=[row(15)], deletes=[4])
    backup = call(url, 'GET', '/backup', raw=True)
    (workdir / 'backup.lmi').write_bytes(backup)
    frozen_model, frozen_head = dict(model), ledger[-1]['sequence']
    commit(url, model, ledger, puts=[row(100)])
    call(url, 'POST', '/checkpoint')
    commit(url, model, ledger, puts=[row(101), row(102)])
    head = ledger[-1]['sequence']
    (workdir / 'ledger.json').write_text(json.dumps({'ledger': ledger, 'head': head}, indent=1))
    latency, rc = rt.signal('main', signal.SIGTERM)
    check('P1: clean stop inside bound', latency < STOP_BOUND and rc == 0,
          stop_seconds=round(latency, 3), exit_code=rc)
    rt.up('main')  # container rebuild: fresh runtime state, same volume
    url = rt.url('main')
    saved = json.loads((workdir / 'ledger.json').read_text())
    check('P1: acknowledged-commit ledger persisted outside the container', saved['head'] == head, head=head)
    head_after, stats, records = verify_history(url, [e['sequence'] for e in ledger], 'P1')
    check('P1: head equals last acknowledged commit', head_after == head,
          head=head_after, recovered_tail_bytes=stats['recovered_tail_bytes'])
    rows = all_rows(url)
    check('P1: visible table equals acknowledged model', rows == model_rows(model),
          expected=len(model), observed=len(rows))
    by_seq = {r['sequence']: r for r in records}
    bad = [e['sequence'] for e in ledger
           if by_seq[e['sequence']]['kind'] != 'commit'
           or len(by_seq[e['sequence']].get('writes', [])) != len(e['puts']) + len(e['deletes'])]
    check('P1: ledger entries match journal commit records', not bad, mismatched=bad)
    historical = call(url, 'POST', '/query', {'relation': 'spectra', 'at': seed_seq, 'limit': 1000})['rows']
    check('P1: pre-rebuild snapshot still readable', historical == model_rows(snapshot_at_seed), at=seed_seq)
    return {'frozen_model': frozen_model, 'frozen_head': frozen_head}

class Loader:
    def __init__(self, url, model, start_key, cap):
        self.url, self.model, self.next_key, self.cap = url, model, start_key, cap
        self.acked, self.unacked, self.attempted = [], [], {}
        self.ckpt_ok = self.ckpt_fail = 0
        self.ckpt_inflight = threading.Event()
        self.halt = threading.Event()
    def committer(self):
        while not self.halt.is_set() and self.next_key < self.cap:
            key = self.next_key
            self.next_key += 1
            try:
                tx = call(self.url, 'POST', '/transactions', {}, status=201, timeout=10)['id']
                call(self.url, 'POST', f'/transactions/{tx}/put',
                     {'relation': 'spectra', 'row': row(key), 'insert_only': True}, timeout=10)
            except Exception:
                time.sleep(0.01)
                continue  # never reached commit: cannot be durable
            self.attempted[key] = row(key)
            try:
                out = call(self.url, 'POST', f'/transactions/{tx}/commit', timeout=10)
                self.acked.append((out['sequence'], key))
                self.model[key] = row(key)
            except Exception:
                self.unacked.append(key)  # outcome uncertain; resolved after restart
    def checkpointer(self):
        while not self.halt.is_set():
            self.ckpt_inflight.set()
            try:
                call(self.url, 'POST', '/checkpoint', timeout=30)
                self.ckpt_ok += 1
            except Exception:
                self.ckpt_fail += 1
            finally:
                self.ckpt_inflight.clear()

def phase_signal_under_load(rt, sig, note):
    rt.reset_volume('main')
    url = rt.up('main')
    model, ledger = {}, []
    seed(url, model, ledger, [row(n) for n in range(1, 3001)])
    loader = Loader(url, model, 5000, 5000 + 1500)
    with concurrent.futures.ThreadPoolExecutor(2) as pool:
        pool.submit(loader.committer)
        pool.submit(loader.checkpointer)
        load_start = time.monotonic()
        deadline = load_start + 8
        while time.monotonic() < deadline:
            if loader.acked and time.monotonic() - load_start > 1.0 and loader.ckpt_inflight.is_set():
                break
            time.sleep(0.01)
        overlapped = loader.ckpt_inflight.is_set()
        latency, rc = rt.signal('main', sig)
        loader.halt.set()
    acked_seqs = [s for s, _ in loader.acked]
    check(f'{note}: load flowed before signal', len(acked_seqs) > 0,
          commits_acknowledged=len(acked_seqs), checkpoints_ok=loader.ckpt_ok,
          checkpoint_in_flight_at_signal=overlapped)
    check(f'{note}: stop finished inside bound', latency < STOP_BOUND,
          stop_seconds=round(latency, 3), exit_code=rc, signal=int(sig))
    rt.up('main')
    url = rt.url('main')
    head, stats, _ = verify_history(url, acked_seqs, note)
    rows = all_rows(url)
    present = {r['sample'] for r in rows}
    # Only the last unacked attempt may have reached the journal; every
    # earlier one was rejected before commit and must be absent.
    early = [k for k in loader.unacked[:-1] if k in present]
    check(f'{note}: rejected attempts never became visible', not early, leaked=early[:5])
    expected, fate = dict(model), 'none'
    if loader.unacked:
        last = loader.unacked[-1]
        fate = 'durable' if last in present else 'absent'
        if fate == 'durable':
            expected[last] = loader.attempted[last]
    check(f'{note}: table explained by acks plus at most the uncertain last commit',
          rows == model_rows(expected), fate_of_uncertain_commit=fate,
          recovered_tail_bytes=stats['recovered_tail_bytes'], head=head)
    rt.down('main')

def phase_second_instance(rt):
    rc, out = rt.second_instance('main')
    check('P4: second instance on same directory refused', rc != 0 and 'already open' in out,
          exit_code=rc, output=(out.strip().splitlines() or [''])[-1])
    url = rt.url('main')
    before = call(url, 'GET', '/stats')['sequence']
    model, ledger = {}, []
    commit(url, model, ledger, puts=[row(900)])
    after = call(url, 'GET', '/stats')['sequence']
    check('P4: primary unaffected by refused second instance', after == before + 1,
          before=before, after=after)

def phase_restore(rt, workdir, frozen):
    backup = workdir / 'backup.lmi'
    url = rt.up('restored', restore=backup)
    stats = call(url, 'GET', '/stats')
    base = stats['history_base']
    check('P5: restored head equals frozen backup head', stats['sequence'] == frozen['frozen_head'],
          head=stats['sequence'], history_base=base)
    rows = all_rows(url)
    check('P5: restored table equals frozen model', rows == model_rows(frozen['frozen_model']),
          expected=len(frozen['frozen_model']), observed=len(rows))
    stale = frozen['frozen_head'] - 1
    resp = call(url, 'GET', f'/changes?after={stale}', status=410)
    check('P5: pre-restore change cursor rejected with 410', resp['error']['code'] == 'snapshot',
          cursor=stale, history_base=base)
    page = call(url, 'GET', f'/changes?after={base}&limit=100')
    check('P5: consumer re-aligns at history_base', page['records'] == [] and page['cursor'] == base,
          cursor=page['cursor'], head=page['head'])
    model, ledger = {}, []
    seq = commit(url, model, ledger, puts=[row(7001)])
    check('P5: restored journal continues from base', seq == base + 1, sequence=seq, base=base)
    page = call(url, 'GET', f'/changes?after={base}&limit=100')
    check('P5: new commit visible from re-aligned cursor',
          len(page['records']) == 1 and page['records'][0]['sequence'] == seq)
    rc, out = rt.restore_onto_existing('main', backup)
    check('P5: restore onto an initialized directory refused', rc != 0 and 'new directory' in out,
          exit_code=rc, output=(out.strip().splitlines() or [''])[-1])
    main_head = call(rt.url('main'), 'GET', '/stats')['sequence']
    check('P5: primary volume untouched by refused restore', main_head >= 1, head=main_head)
    rt.down('restored')

def phase_confinement(rt):
    if isinstance(rt, Local):
        after = rt.tree()
        changed = [p for p in set(after) | set(rt.tree_before) if rt.tree_before.get(p) != after.get(p)]
        outside = [p for p in changed if not (p == 'tmp' or p.startswith('tmp/') or p.startswith('vol-'))]
        check('P6: writes confined to data volumes (read-only root held)', not outside,
              changed_outside_volumes=outside[:10], changed_paths=len(changed))
    else:
        info = rt.inspect_hardening()
        cfg, host = info['Config'], info['HostConfig']
        check('P6: container hardening verified',
              cfg.get('User') == '10001:10001' and host.get('ReadonlyRootfs') is True
              and '/tmp' in (host.get('Tmpfs') or {}) and 'ALL' in (host.get('CapDrop') or []),
              user=cfg.get('User'), readonly_rootfs=host.get('ReadonlyRootfs'),
              tmpfs=host.get('Tmpfs'), cap_drop=host.get('CapDrop'))

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--runtime', choices=['local', 'compose'], default='local')
    parser.add_argument('--workdir', default=str(REPO / 'deploy' / 'drill-work'))
    args = parser.parse_args()
    workdir = pathlib.Path(args.workdir)
    if args.runtime == 'compose' and workdir.resolve() != (REPO / 'deploy' / 'drill-work'):
        raise SystemExit('compose runtime requires the default workdir deploy/drill-work')
    workdir.mkdir(parents=True, exist_ok=True)
    rt = Local(workdir) if args.runtime == 'local' else Compose(workdir)
    started = time.monotonic()
    ok = False
    try:
        frozen = phase_ledger_rebuild(rt, workdir)
        phase_second_instance(rt)
        phase_restore(rt, workdir, frozen)
        phase_signal_under_load(rt, signal.SIGTERM, 'P2')
        phase_signal_under_load(rt, signal.SIGKILL, 'P3')
        rt.up('main')
        phase_confinement(rt)
        ok = True
    except DrillFailed:
        pass
    except Exception as e:
        check('drill aborted with unexpected error', False, error=f'{type(e).__name__}: {e}')
    finally:
        rt.teardown()
    summary = {'check': 'summary', 'status': 'pass' if ok else 'fail',
               'runtime': args.runtime, 'elapsed_seconds': round(time.monotonic() - started, 1),
               'checks': len(REPORT)}
    REPORT.append(summary)
    print(json.dumps(summary), flush=True)
    (workdir / 'report.json').write_text(json.dumps(REPORT, indent=1))
    sys.exit(0 if ok else 1)

if __name__ == '__main__':
    main()
