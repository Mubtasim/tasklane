# Lab: Helm, hands on

Install Tasklane from the published chart into a local Kubernetes cluster, then upgrade it, change a setting, look at the history, roll back and uninstall, watching the pods change live. About 30–40 minutes and about 0.9 GB of RAM (one k3d cluster). The outputs below are from a dry run on a 2-core laptop with a locally built image; yours will differ in names and seconds.

**What Helm is, in one paragraph.** A *chart* is a folder of Kubernetes YAML *templates* with blanks (`{{ .Values.replicaCount }}`), a `values.yaml` with the default for every blank, and a `Chart.yaml` label (chart version, app version). `helm install` fills the blanks with your values and applies the result as one named *release*. Helm remembers every change as a numbered *revision*, so it can show the history, roll back, and uninstall exactly what it created. Tasklane's chart lives in [`deploy/k8s/tasklane/`](../../deploy/k8s/tasklane/) and is published with each release to `oci://ghcr.io/mubtasim/charts/tasklane`.

## 0. Setup

Tools (pinned versions into `~/.local/bin`), from the repo root:

```sh
deploy/k8s/tools.sh kubectl helm k3d
export PATH="$HOME/.local/bin:$PATH"
source deploy/k8s/versions.sh          # sets K3S_IMAGE
export V=1.0.0-rc.2                    # the release to install
```

Two terminal panes (tmux: `Ctrl-b %`). In the **right** pane, after step 1, keep this running the whole time:

```sh
kubectl get pods -w
```

`-w` = watch: every change to a pod prints a new line. Most of what you learn is in that pane.

## 1. A cluster and a database

```sh
k3d cluster create helm-lab --image "$K3S_IMAGE" --agents 0 --wait
kubectl create namespace tasklane
kubectl config set-context --current --namespace tasklane
kubectl apply -f deploy/k8s/demo/postgres.yaml
kubectl rollout status statefulset/postgres
```

The chart doesn't include Postgres (a real install uses a managed database), so the demo's one-pod Postgres stands in. Now start the watch in the right pane: `postgres-0  1/1 Running`.

**Did you notice?** `helm list -A` already shows two releases, `traefik` and `traefik-crd`: k3s installs its own ingress controller *with Helm*. That is the most common use of Helm: installing other people's software.

## 2. Install

```sh
helm install tasklane oci://ghcr.io/mubtasim/charts/tasklane --version "$V" \
  --set databaseURL='postgres://tasklane:tasklane@postgres:5432/tasklane?sslmode=disable' \
  --set apiKeys=lab-key \
  --wait
```

- `tasklane` (the first one) is the **release name**: every object is named after it (`tasklane-server`, `tasklane-envoy`).
- `--set` fills two blanks; everything else comes from the chart's `values.yaml`.
- `--wait` returns only when every pod is Ready.

Expect, in about 20 s:

```
STATUS: deployed
REVISION: 1
NOTES:
Tasklane is starting: 2 servers behind Envoy.
Clients connect to tasklane.tasklane.svc.cluster.local:7070 (gRPC) or :8080 (HTTP).
```

**Watch pane:** two `tasklane-server-…` and two `tasklane-envoy-…` pods go `Pending → ContainerCreating → Running`, then `1/1` once the readiness probe (`/readyz`) passes. Kubernetes **downloaded the image from GHCR** by itself: check with

```sh
kubectl get deploy tasklane-server -o jsonpath='{.spec.template.spec.containers[0].image}'; echo
# ghcr.io/mubtasim/tasklane:1.0.0-rc.2  ← the chart's appVersion
```

## 3. Send jobs

Open a tunnel from your laptop to the `tasklane` Service (Envoy), in the background:

```sh
kubectl port-forward svc/tasklane 8080:8080 >/dev/null 2>&1 &
H='Authorization: Bearer lab-key'
curl -s -H "$H" -d '{"name":"lab"}' localhost:8080/v1/queues
for i in 1 2 3; do curl -s -H "$H" -d "{\"kind\":\"hello\",\"payload\":{\"n\":$i}}" localhost:8080/v1/queues/lab/jobs; echo; done
curl -s -H "$H" localhost:8080/v1/queues/lab | jq -c .stats
```

```
{"job_id":"1", "created":true}
...
{"available":"3","leased":"0","dead":"0","recently_completed":"0","recent_window":"10s"}
```

Play worker by hand: lease one job and acknowledge it.

```sh
LEASE=$(curl -s -H "$H" -d '{"max_jobs":1,"wait":"5s"}' localhost:8080/v1/queues/lab:lease)
ID=$(jq -r '.jobs[0].id' <<<"$LEASE"); TOKEN=$(jq -r '.jobs[0].lease_token' <<<"$LEASE")
curl -s -H "$H" -d "{\"lease_token\":\"$TOKEN\"}" "localhost:8080/v1/jobs/${ID}:ack"; echo
curl -s -H "$H" "localhost:8080/v1/jobs/${ID}" | jq -r .job.state      # JOB_STATE_COMPLETED
curl -s -o /dev/null -w '%{http_code}\n' localhost:8080/v1/queues/lab  # 401: no key
```

Write `${ID}:ack`, with braces: in zsh, `$ID:a…` is a modifier (`:a` makes an absolute path) and the request goes to the wrong URL.

## 4. Scale up: `helm upgrade`

```sh
helm upgrade tasklane oci://ghcr.io/mubtasim/charts/tasklane --version "$V" --reuse-values --set replicaCount=3 --wait
```

`--reuse-values` keeps what you set before (database, key) and changes only `replicaCount`.

**Watch pane:** **one** new `tasklane-server-…` pod appears; the other pods don't restart. Only the Deployment's `replicas` changed, so Kubernetes only adds a pod. `REVISION: 2`.

## 5. Change a setting: a rolling update

```sh
helm upgrade tasklane oci://ghcr.io/mubtasim/charts/tasklane --version "$V" --reuse-values --set apiKeys=new-key --wait
```

**Watch pane**, the important part. From the dry run:

```
tasklane-server-5d59b89d8-fvts5    0/1     Pending             0          0s
tasklane-server-5d59b89d8-fvts5    0/1     ContainerCreating   0          0s
tasklane-server-5d59b89d8-fvts5    1/1     Running             0          2s   ← new pod Ready
tasklane-server-5fc5996559-t8wq7   1/1     Terminating         0          30s  ← only then an old one stops
tasklane-server-5d59b89d8-rlbvn    0/1     Pending             0          0s
...                                                                              (three times)
```

- The key lives in a Secret. The pod template carries a **hash of that Secret** (`checksum/secret`), so a new key changes the template, which makes Kubernetes replace the pods.
- New pods have a new name part (`5d59b89d8` vs `5fc5996559`): a new ReplicaSet.
- **One at a time, new before old:** the chart sets `maxSurge: 1, maxUnavailable: 0`, so there are never fewer than 3 ready servers. A stopping pod keeps serving for 5 s (`preStop`) while it is removed from the Service, then drains.

Check that it took effect (restart the tunnel: the pod behind it is gone):

```sh
kill %1; kubectl port-forward svc/tasklane 8080:8080 >/dev/null 2>&1 &
for k in lab-key new-key; do curl -s -o /dev/null -w "$k: %{http_code}\n" -H "Authorization: Bearer $k" localhost:8080/v1/queues/lab; done
# lab-key: 401
# new-key: 200
```

## 6. History

```sh
helm history tasklane
```

```
REVISION  STATUS      CHART                APP VERSION  DESCRIPTION
1         superseded  tasklane-1.0.0-rc.2  1.0.0-rc.2   Install complete
2         superseded  tasklane-1.0.0-rc.2  1.0.0-rc.2   Upgrade complete
3         deployed    tasklane-1.0.0-rc.2  1.0.0-rc.2   Upgrade complete
```

Every deploy is a numbered revision, with the chart and app version it used.

## 7. Roll back

```sh
helm rollback tasklane 1 --wait
helm history tasklane | tail -2     # 4  deployed  ...  Rollback to 1
```

**Watch pane:** back to 2 servers, with the old ReplicaSet name (`5fc5996559`) and the old key. A rollback is **a new revision (4)** that copies revision 1; history is never rewritten.

```sh
kill %1; kubectl port-forward svc/tasklane 8080:8080 >/dev/null 2>&1 &
for k in lab-key new-key; do curl -s -o /dev/null -w "$k: %{http_code}\n" -H "Authorization: Bearer $k" localhost:8080/v1/queues/lab; done
# lab-key: 200
# new-key: 401
curl -s -H "Authorization: Bearer lab-key" localhost:8080/v1/queues/lab | jq -c .stats   # your jobs are still there: they live in Postgres
```

## 8. What Helm actually sent

```sh
helm get values tasklane                  # only what you set (USER-SUPPLIED VALUES)
helm get manifest tasklane | less         # the finished YAML, blanks filled in
helm get manifest tasklane | grep -E '^kind:|replicas:|image:'
```

The manifest holds 2 PodDisruptionBudgets, a Secret, a ConfigMap (Envoy's config), Services and 2 Deployments (servers and Envoy), about 400 lines. Compare one template with its output: `deploy/k8s/tasklane/templates/server-deployment.yaml` has `replicas: {{ .Values.replicaCount }}`; the manifest has `replicas: 2`.

## 9. Uninstall and clean up

```sh
kill %1
helm uninstall tasklane --wait
kubectl get all              # only postgres is left: it wasn't part of the release
k3d cluster delete helm-lab
```

Helm deleted exactly the objects it created and nothing else.

## What to take away

- **Chart = templates + values + label.** Environments differ only in values.
- **Release = one install of a chart.** Its name prefixes every object.
- **`upgrade` changes only what changed.** Scaling adds pods; a template change (here, the Secret hash) rolls them, new before old.
- **Every change is a revision.** `history` shows them, `rollback` adds a new one copying an old one, `uninstall` removes exactly what Helm made.
- Interview notes: [Q59](../interview-notes.md#q59-what-is-a-helm-chart-and-why-do-teams-use-helm-instead-of-plain-yaml).
