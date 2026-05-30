# CKIC

> Amateur Kubernetes operator that runs a dedicated [Caddy](https://caddyserver.com) instance on every selected node and keeps each instance's configuration continuously in sync.

## Technical Structure

```mermaid
flowchart LR
    subgraph Manager["<b><code>ckic-manager</code></b>"]
        direction TB
        Entry["<b><code>cmd/manager</code></b><br/>CLI flags + leader election + health probes"]
        subgraph Ctrl["<b><code>pkg/controller</code></b>"]
            direction TB
            Informers["<b>Informers</b><br/>nodes + <code>ConfigMaps</code> + <code>Deployments</code>"]
            Reconciler["<b>Workqueue</b> + <b>reconciler</b><br/>server-side apply + push dedup + redeploy-on-failure"]
        end
        Aggregator["<b><code>pkg/aggregator</code></b><br/>merge base + external <code>Caddyfile</code>s"]
        subgraph CaddyPkg["<b><code>pkg/caddy</code></b>"]
            direction TB
            Deployer["<b>Deployer</b><br/>SSA <code>Deployment</code> and <code>Service</code> + image prepull"]
            Admin["<b>Admin client</b><br/>push config via <code>/load</code>"]
        end
        Entry -->|"run <i>(leader only)</i>"| Reconciler
        Informers -->|"enqueue node and config keys"| Reconciler
        Informers -->|"base and external fragments"| Aggregator
        Aggregator -->|"merged <code>Caddyfile</code>"| Reconciler
        Reconciler -->|"per managed node"| Deployer
        Reconciler -->|"push merged config"| Admin
    end
    subgraph Cluster["<b>Kubernetes cluster</b>"]
        direction TB
        K8s["<b>Kubernetes API</b>"]
        ConfigMaps[("<b>base</b> + <b>external</b><br/><code>ConfigMap</code>s")]
        MirrorCM[("<b>merged mirror</b><br/><code>ConfigMap</code>")]
        Instances["<b>Caddy instances</b><br/>one <code>Deployment</code> and <code>Service</code> per labelled node"]
        LB["<b>LoadBalancer</b><br/><code>none</code> / <code>cilium</code> / <code>shared</code>"]
    end
    Informers <-->|"watch nodes and <code>Deployment</code>s"| K8s
    ConfigMaps -->|"watched"| Informers
    Aggregator -->|"publish (SSA)"| MirrorCM
    Deployer -->|"apply objects (SSA)"| K8s
    K8s -->|"schedules"| Instances
    Admin -->|"push via admin API"| Instances
    Deployer -.-> LB
    LB -->|"traffic"| Instances
```
