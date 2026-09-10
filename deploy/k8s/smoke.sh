#!/usr/bin/env bash
# Cluster smoke test: the scaler must drive worker replicas toward demand.
set -euo pipefail
NS=tasker
CLI="cargo run -q -p tasker-cli --"
ROOT=$(git rev-parse --show-toplevel)
cd "$ROOT"

say() { printf '\n== %s\n' "$*"; }
replicas() { kubectl -n $NS get deploy tasker-worker -o jsonpath='{.status.readyReplicas}' 2>/dev/null || echo 0; }
desired() { curl -fs localhost:9090/metrics | awk '/^tasker_workers_desired /{print $2}'; }

say "waiting for taskerd"
kubectl -n $NS rollout status deploy/taskerd --timeout=120s >/dev/null

say "port-forward 7070 (grpc) and 9090 (metrics)"
# A local daemon on the same ports would silently receive every submit below.
if lsof -nP -iTCP:7070 -sTCP:LISTEN >/dev/null 2>&1 || lsof -nP -iTCP:9090 -sTCP:LISTEN >/dev/null 2>&1; then
  echo "FAIL: port 7070 or 9090 is already in use locally (a local taskerd?); stop it first"; exit 1
fi
kubectl -n $NS port-forward svc/taskerd 7070:7070 9090:9090 >/dev/null 2>&1 &
PF=$!
trap 'kill $PF 2>/dev/null || true' EXIT
sleep 2

# Prove we are talking to the cluster daemon: its worker is a pod.
$CLI nodes | grep -q "tasker-worker-" || { echo "FAIL: the daemon on 7070 has no pod worker; is this the cluster?"; exit 1; }
say "baseline: replicas=$(replicas) desired=$(desired)"
[ "$(replicas)" -ge 1 ] || { echo "FAIL: no worker ready"; exit 1; }

say "submitting 40 jobs x 3000 millicores x 20 s"
for _ in $(seq 40); do $CLI submit --sleep 20000 --cpu-millis 3000 --walltime 120 >/dev/null; done
sleep 1
say "demand now: desired=$(desired) (expect 8, the max)"
[ "$(desired)" -ge 4 ] || { echo "FAIL: demand did not rise"; exit 1; }

say "waiting up to 90 s for KEDA to scale up"
for _ in $(seq 90); do
  r=$(replicas); [ "${r:-0}" -ge 4 ] && break; sleep 1
done
say "replicas after scale-up: $(replicas)"
[ "$(replicas)" -ge 4 ] || { echo "FAIL: replicas stayed at $(replicas)"; exit 1; }

say "waiting for the queue to drain"
for _ in $(seq 120); do
  q=$($CLI queue); echo "  $q"
  [[ "$q" == "ready 0  blocked 0  running 0"* ]] && break; sleep 5
done

say "waiting up to 120 s for scale-down after cooldown"
for _ in $(seq 120); do
  r=$(replicas); [ "${r:-0}" -le 1 ] && break; sleep 1
done
say "replicas after scale-down: $(replicas) desired=$(desired)"
[ "$(replicas)" -le 1 ] || { echo "FAIL: did not scale down"; exit 1; }
echo; echo "PASS"
