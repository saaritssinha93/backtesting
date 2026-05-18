learni# OBSERVABILITY / DEVOPS INTERVIEW — ANSWER SHEET

JD Focus: Monitoring & Observability, Prometheus, Grafana, ELK/EFK, Kubernetes, AWS, OpenTelemetry, New Relic/APM, Terraform, Jenkins, Scripting, Linux, CI/CD

> Personal/biographical questions (Section A Q1, Section 1) are left for you to answer in your own words. Everything else is answered below.

---

## SECTION A — KEY INTERVIEWER QUESTIONS

**1. Tell me about yourself.** *(Personal — answer in your own words: education, total years of experience, current role, key tools, and one production-impact achievement.)*

**2. Difference between a monolithic service and a microservice.**
A monolith packages all business modules (UI, business logic, data access) into one deployable unit sharing a single process and database; scaling means scaling the whole app. Microservices split functionality into small, independently deployable services that communicate via APIs/queues, each owning its data store. Microservices give independent scaling, polyglot tech stacks, and team autonomy, but add network, observability, and consistency complexity.

**3. Difference between Prometheus and Grafana. How will you create a dashboard in Grafana?**
Prometheus is a time-series database and metrics collection engine — it scrapes targets, stores metrics, and runs alerting via Alertmanager. Grafana is a visualization layer that queries data sources (Prometheus, Elasticsearch, CloudWatch, Loki) and renders dashboards/alerts. To create a Grafana dashboard: add Prometheus as a data source (Configuration → Data Sources), click **+ → Dashboard → Add new panel**, write a PromQL query (e.g., `rate(http_requests_total[5m])`), choose visualization (graph/stat/gauge), set thresholds and units, save the panel and dashboard, then add template variables for ticker/instance/namespace to make it reusable.

**4. Project architecture.** *(Personal — describe your architecture: data flow from app → Prometheus/Fluent Bit → Elasticsearch/Grafana → Alertmanager → PagerDuty/Slack, layered on EKS with Terraform-provisioned infra and Jenkins CI/CD.)*

**5. Kubernetes architecture.**
A cluster has a **control plane** and **worker nodes**. Control plane: **API Server** (REST front-end), **etcd** (cluster state KV store), **Scheduler** (assigns pods to nodes), **Controller Manager** (reconciles desired state — Deployment, ReplicaSet, Node controllers), and **Cloud Controller Manager** (cloud-specific integration). Worker nodes run **kubelet** (talks to API server, manages pods), **kube-proxy** (network rules/load balancing for Services), and a container runtime (containerd, CRI-O). Users apply YAML to API Server → controllers reconcile → Scheduler binds pods → kubelet pulls images and runs containers.
 
**6. What is kubeconfig?**
A YAML file (default `~/.kube/config`) that tells `kubectl` how to authenticate and which cluster to talk to. It has three sections: **clusters** (API server URL + CA cert), **users** (auth credentials — cert, token, exec plugin like `aws eks get-token`), and **contexts** (cluster + user + namespace tuple). `kubectl config use-context <name>` switches between environments.

**7. How will you troubleshoot Kubernetes errors?**
Start with `kubectl get pods -n <ns>` to identify the failing pod and status (Pending/CrashLoopBackOff/ImagePullBackOff). Run `kubectl describe pod <pod>` for events (image pull, scheduling failure, probe failure, OOMKilled), `kubectl logs <pod> -p` for previous-container logs on a crash loop, and `kubectl get events --sort-by=.lastTimestamp` for cluster-wide signals. Then check node health (`kubectl describe node`), resource quotas, RBAC, ConfigMap/Secret references, image registry auth, and network/DNS via an `nslookup` from a debug pod.

**8. What is Terraform? Explain taint and drift.**
Terraform is a declarative IaC tool from HashiCorp that uses HCL to provision cloud/SaaS resources idempotently via providers; it stores actual infrastructure state in a state file. **Taint** (`terraform taint`, now `-replace` flag) marks a resource for forced recreation on the next apply — used when a resource is in a bad state but config is unchanged. **Drift** is when real infrastructure diverges from the state file (e.g., someone edited a Security Group in the AWS console); `terraform plan` detects drift, and `terraform apply` reconciles it back to the declared config (or `terraform refresh`/`import` to sync state).

**9. Jenkins pipeline. Master and slave/controller.**
A Jenkins pipeline is a CI/CD workflow defined in a `Jenkinsfile` (declarative or scripted Groovy) checked into source control. It has **stages** (Checkout, Build, Test, Scan, Deploy) and **steps** within each stage. **Controller** (formerly *master*) manages job orchestration, scheduling, UI, and config. **Agents** (formerly *slaves*) are worker nodes (VMs, containers, Kubernetes pods) that actually execute build steps. The controller dispatches jobs to agents over SSH/JNLP — keeps the controller lightweight and lets you scale builds horizontally.

**10. How does Jenkins communicate with a cloud platform?**
Jenkins authenticates to clouds via credentials configured in **Manage Jenkins → Credentials**. For AWS: store an IAM access key/secret as `aws-credentials`, or (preferred) attach an **IAM role** to the Jenkins EC2 instance / use **IRSA** if Jenkins runs on EKS — pipelines call `aws` CLI / `terraform` / `kubectl` and the SDK auto-discovers the role. Plugins (AWS Steps, Kubernetes, Azure CLI, GCP) wrap these calls. Communication is over HTTPS to the cloud provider's API endpoint.

---

## SECTION 1 — ABOUT YOURSELF / PROJECT
*(Most of this section is personal/role-based. Answer with your real project specifics. Below are skeletons for the role-experience questions.)*

**1–12 (Easy, personal):** Tell me about yourself / current project / role / day-to-day / tools / apps you monitor / experience in monitoring, cloud, CI/CD, K8s, incident, on-call. → *Answer using your actual project.*

**13. Project architecture end-to-end.** *(Personal — describe your real architecture.)* Generic template: users → CDN/WAF → ALB → EKS-hosted microservices → RDS/DynamoDB → SQS/Kafka. Observability: Prometheus + node-exporter + kube-state-metrics scrape pods (ServiceMonitor); Fluent Bit DaemonSet ships logs to Elasticsearch; OTel Collector forwards traces to Jaeger/New Relic; Grafana dashboards + Alertmanager → PagerDuty/Slack. CI/CD: Jenkins multibranch pipelines build → ECR → Helm-deploy to EKS; infra in Terraform.

**14. Monitoring solution implemented.**
Prometheus + Grafana + Alertmanager for metrics; EFK (Fluent Bit + Elasticsearch + Kibana) for logs; OpenTelemetry + Jaeger/New Relic for traces. Synthetic checks via Blackbox Exporter; APM via New Relic agent. Dashboards organized as folders per team; alert routes per severity → on-call.

**15. How do you collect metrics, logs and traces?**
Metrics: Prometheus scrapes `/metrics` endpoints exposed by app (client libraries) and exporters (node, kube-state, blackbox). Logs: Fluent Bit DaemonSet tails `/var/log/containers/*.log`, enriches with Kubernetes metadata, ships to Elasticsearch. Traces: OpenTelemetry SDK auto-instrumented in the app sends OTLP spans to OTel Collector → backend (Jaeger/New Relic).

**16. Dashboards created.**
Examples: cluster health (node CPU/mem/disk, pod restarts, pending pods), app SLI (RED — Rate, Errors, Duration), business KPI (orders/min, login success rate), Java GC/heap, RDS connection/IOPS, ALB 5xx rate, Kafka consumer lag.

**17. How do you define critical alerts?**
Tied to SLOs and customer impact. Severity-1: customer-facing 5xx > 1% for 5m, p95 latency breach, full outage. Severity-2: capacity (disk > 85%, cert expiring < 7d). Always include runbook link, owning team, and a *symptom-based* condition (not raw infra metric) to avoid noise.

**18. How do you handle production incidents?**
Acknowledge alert → triage severity → open incident bridge → freeze deployments → identify suspect change (recent deploy, infra event) → mitigate (rollback, scale, failover) → confirm recovery via dashboards → communicate status to stakeholders → write postmortem with timeline, RCA, and action items.

**19. RCA approach.**
Use the *5 Whys* combined with timeline correlation: gather metrics, logs, traces, deploy events, infra changes for the incident window; identify the first observable anomaly; trace causality from symptom → component → trigger; verify with a reproducer if possible; categorize as code defect, config drift, capacity, or external dependency.

**20. Most challenging production issue.** *(Personal — pick a real story with: symptom, hypothesis path, tools used (Grafana/Kibana/kubectl), root cause, fix, prevention.)*

**21. Coordination with app/infra/business.**
Daily standups, shared Slack channels, Confluence runbooks, joint war rooms during incidents. Translate technical issues into business impact (revenue, customer count, SLA risk) for leadership; keep dev teams looped via Jira tickets and weekly observability reviews.

**22. Ensuring monitoring is fit for purpose.**
Map monitoring to user journeys and SLOs, not infra metrics alone. Review false-positive rates monthly. Validate every alert has a runbook and owner. Run game days / chaos drills to confirm alerts fire when they should.

**23. Non-functional requirements for observability.**
Availability (HA Prometheus, multi-AZ ES), retention (cost vs compliance), cardinality limits, security (TLS, RBAC, PII redaction), data sovereignty, latency of alerting (< 1 min), scalability, vendor lock-in avoidance, audit trail.

**24. Designing observability for a large enterprise.**
Three pillars + correlation: Prometheus (federated or Thanos for global view), centralized ES/OpenSearch with ILM tiers, OTel Collector pipelines feeding multiple backends. Multi-tenant via namespaces/labels. Use service catalog for ownership mapping. Standard SDK + lib injection at platform layer so apps get instrumentation free.

**25. Monitoring 500+ microservices across multiple clusters.**
Per-cluster Prometheus + Thanos sidecar for global query; common label set (`cluster`, `service`, `team`, `env`); ServiceMonitor CRDs auto-discover; Grafana dashboards driven by variables (no per-service dashboard) — one template renders for all. Alerts authored as PrometheusRule CRDs, owned by service teams via GitOps.

**26. Reduce false-positive alerts.**
Symptom-based alerts on SLOs, not raw thresholds. Use `for: 5m` to avoid flaps. Multi-condition: latency AND error rate. Drop noisy infra alerts that don't impact users. Burn-rate alerts (multi-window). Quarterly alert audit — remove if not actioned in 90 days.

**27. Executive vs technical dashboards.**
Executive: one screen, business KPIs (revenue/min, transactions/sec, regional uptime, SLA %), trend over weeks, no PromQL. Technical: deep-dive RED/USE, drill-down with variables, log links, trace links, raw metric panels.

**28. Decide alert thresholds.**
Baseline from historical data (p95/p99), align to SLO error budgets, use multi-window burn-rate (e.g., 14.4× SLO budget over 1h triggers fast). Validate with a backtest of recent incidents.

**29. Alert fatigue.**
Too many alerts, low signal-to-noise → on-call ignores everything. Caused by raw-infra alerts, no `for:` window, duplicate alerts, missing routing. Fix by SLO-based alerting and aggressive deletion.

**30. Improve a monitoring platform with too many noisy alerts.**
Audit the last 90 days of alerts → categorize: actionable / informational / noise. Delete noise. Convert informational to dashboards/reports. Tune thresholds with `for:` and burn-rate. Add deduplication and inhibition in Alertmanager. Track MTTA/MTTR weekly.

**31. Prove business value from observability.**
Track MTTD/MTTR reduction, incident frequency, error budget consumption, deploy success rate. Dollar-value impact: minutes of downtime avoided × revenue/min. Customer-experience: synthetic check uptime, p95 page load.

**32. Migrate from legacy monitoring to modern observability.**
Inventory existing alerts/dashboards/metrics. Stand up new stack in parallel. Migrate top 20% high-value dashboards first. Bridge alerts (dual-route) until trust built. Train teams. Decommission legacy after 90-day stable window.

---

## SECTION 2 — MONITORING & OBSERVABILITY BASICS

### Easy

**1. Monitoring.** Continuously collecting predefined metrics/logs from systems and alerting when known thresholds are breached. Answers "is the system OK?" against expected failure modes.

**2. Observability.** The ability to infer the *internal state* of a system from its external outputs (metrics, logs, traces). Lets you answer questions you didn't pre-define — "why is this slow now?".

**3. Difference monitoring vs observability.** Monitoring = known-unknowns (predefined alerts). Observability = unknown-unknowns (exploratory debugging). Monitoring is a subset of observability.

**4. Metrics.** Numeric measurements collected over time (CPU%, request count, latency). Cheap to store, ideal for dashboards and alerts.

**5. Logs.** Timestamped, structured or unstructured text records of discrete events from applications/systems. High-cardinality, useful for forensic analysis.

**6. Traces.** End-to-end record of a single request as it traverses multiple services; composed of spans showing duration and parent-child relationships.

**7. Alert.** An automated notification triggered when a metric/condition breaches a rule (e.g., error rate > 5%).

**8. Dashboard.** A visual collection of panels showing metrics/logs/traces for a system or service.

**9. Uptime monitoring.** Checks whether a service/endpoint is reachable and responding correctly (synthetic HTTP/TCP/ICMP checks).

**10. Infrastructure monitoring.** Tracks host-level metrics: CPU, memory, disk, network, processes.

**11. Application monitoring.** Tracks app-internal metrics: request rate, latency, error rate, queue depth, business KPIs.

**12. Synthetic monitoring.** Scripted, scheduled fake user transactions against an endpoint to validate availability/performance from outside.

**13. Real-user monitoring (RUM).** Captures actual end-user browser/mobile interactions — page load, JS errors, API latency from the user's perspective.

**14. APM (Application Performance Monitoring).** End-to-end app-level monitoring: traces, transactions, slow queries, error rates, code-level profiling. Tools: New Relic, Datadog APM, Dynatrace, AppDynamics.

**15. Log aggregation.** Centralizing logs from many sources into one searchable system (ELK/EFK, Loki, Splunk).

**16. Centralized logging.** Same as log aggregation — single pane of glass for all logs.

**17. Incident management.** The structured process of detecting, responding to, mitigating, and learning from production issues.

**18. Root cause analysis (RCA).** Systematic investigation to find the underlying cause (not just symptom) of an incident, usually documented in a postmortem.

### Medium

**19. Three pillars of observability.** **Metrics** (aggregated numerics, cheap, long retention), **Logs** (event records, high detail), **Traces** (request flow across services). Modern view adds **events** and **profiles**.

**20. Why logs alone are not enough.** Logs are high-volume, expensive to query at scale, lack pre-aggregation for trends, and don't show cross-service request flow. Metrics give cheap aggregation; traces give causality.

**21. Why metrics matter.** Cheap to store at fine granularity, ideal for dashboards/alerts/SLO tracking, low query latency, long retention.

**22. Why traces matter in microservices.** A single user request crosses many services; logs alone can't show end-to-end latency or which hop failed. Traces with a shared `trace_id` reveal the bottleneck and failure point.

**23. RED method.** **R**ate (requests/sec), **E**rrors (failed requests/sec), **D**uration (latency distribution). Best for request-driven services. Coined by Tom Wilkie.

**24. USE method.** **U**tilization, **S**aturation, **E**rrors — for resources (CPU, memory, disk, network). Coined by Brendan Gregg. Best for infra components.

**25. SLI, SLO, SLA.** **SLI** = Service Level Indicator (measured quantity, e.g., success rate). **SLO** = Service Level Objective (target for the SLI, e.g., 99.9%). **SLA** = Service Level Agreement (contractual obligation with penalty if SLO breached).

**26. SLI vs SLA.** SLI is the measurement, SLA is the legal/business commitment. SLOs sit between: internal targets, usually stricter than SLA.

**27. Alert fatigue.** When teams receive too many low-value alerts and start ignoring them — leading to missed real incidents.

**28. Event correlation.** Linking related signals across systems (a deploy event + spike in errors + pod restart) to identify causality and reduce noise.

**29. Golden signals.** Google SRE's four: **Latency, Traffic, Errors, Saturation**. Covers most user-facing services.

**30. MTTR.** Mean Time To Recover/Repair — average time from incident start to resolution.

**31. MTTD.** Mean Time To Detect — average time from incident occurrence to detection.

**32. Measure reliability.** Via SLI/SLO compliance, error budget burn rate, MTTD/MTTR, incident frequency, availability %.

**33. Decide what to monitor.** Start from user journeys → identify SLIs → instrument those. Use RED for services, USE for resources, plus business KPIs.

**34. Runbook.** A step-by-step document on how to handle a specific alert/incident: diagnosis steps, mitigation commands, escalation path.

**35. Postmortem.** Post-incident document with timeline, root cause, impact, corrective actions, lessons learned. Should be blameless.

### Difficult

**36. Designing observability for microservices.**
Standardize: shared trace propagation (W3C Trace Context), structured JSON logs with `trace_id`/`span_id`, common metric naming (`service`, `endpoint`, `method`, `status`). Deploy OTel Collector as DaemonSet/Sidecar for receive-process-export. Use ServiceMonitor for auto-discovery. Centralize in Prometheus/Thanos + ES + Jaeger. Build per-service RED dashboards from a single template.

**37. Correlate metrics, logs, traces during outage.**
All three should share a `trace_id`. Start at metric anomaly → click through to log lines for the same time window/service → from a failing log line, jump to its trace_id → see the upstream/downstream spans. Grafana's Tempo/Loki integration and New Relic do this natively.

**38. Define SLOs for customer-facing app.**
Pick SLIs tied to user experience: availability (HTTP 2xx/3xx ratio), latency (p99 < 500ms), correctness (data consistency check). Set SLO over a rolling 28/30-day window. Compute error budget = (1 − SLO) × total events. Alert on burn rate, not raw breaches.

**39. Alerting rules for HA systems.**
Multi-window multi-burn-rate alerts (Google SRE workbook): fast burn (1h × 14.4×) for paging, slow burn (6h × 6×) for ticket. Add inhibition rules so a parent alert silences child alerts. Group alerts by service/cluster to avoid storm.

**40. Reduce MTTR using observability.**
Pre-built runbooks, alerts with runbook URLs, dashboards linked from alerts, trace-log correlation, automated remediation hooks (auto-scale, auto-restart for known patterns), shared incident channels with on-call playbook.

**41. Monitor business KPIs alongside technical metrics.**
Emit business events as Prometheus counters or push to a metrics gateway (orders, signups, revenue). Build mixed dashboards: top — business KPIs; middle — RED metrics; bottom — infra. Alert on business-KPI deviations not just tech.

**42. Identify infra vs application issue.**
Check infra dashboards first (node CPU/mem/disk, network, RDS): if green and app metrics red → application. If infra red and propagating → infra. Traces show whether latency is in app code or downstream calls (DB, external API).

**43. Monitor distributed systems.**
Distributed tracing for request flow, per-service RED metrics, log aggregation with shared correlation IDs, queue/lag metrics for async paths, dependency maps (service mesh telemetry like Istio/Linkerd).

**44. Observability for hybrid cloud.**
Use OTel as the vendor-neutral standard so signals from on-prem and cloud converge. Run regional Prometheus, federate via Thanos/Cortex. Centralize logs in cloud-side ES with Fluent Bit forwarders. Single Grafana with multi-data-source per region.

**45. Prevent monitoring system from being a SPOF.**
Run Prometheus in HA pairs with deduplication. ES/OpenSearch multi-master, multi-AZ. Alertmanager cluster with gossip. Cross-region replication for long-term storage. Independent synthetic monitor (external SaaS) that watches the watcher.

---

## SECTION 3 — PROMETHEUS

### Easy

**1. Prometheus.** Open-source time-series database and metrics collection system, originally built at SoundCloud, now a CNCF graduated project. Pull-based scraping over HTTP, multi-dimensional data model with labels, PromQL query language.

**2. Why used.** Reliable metrics collection with no external dependencies, powerful query language, native Kubernetes integration, mature ecosystem (exporters, Alertmanager, Grafana).

**3. Database type.** Custom time-series database (TSDB) — local, append-only, with WAL, compressed chunk files on disk. Not relational.

**4. Metric.** A named numeric measurement with a set of key=value labels and a timestamp, e.g., `http_requests_total{method="GET",status="200"} 1027`.

**5. Scrape.** Prometheus pulling metrics by HTTP GET on a target's `/metrics` endpoint.

**6. Scrape interval.** How often Prometheus polls a target (default 15s). Set globally and overridable per job.

**7. Exporter.** A bridge that exposes metrics from a system that doesn't natively speak Prometheus format (e.g., MySQL Exporter, Node Exporter).

**8. Node Exporter.** Exporter for Linux/Unix host metrics — CPU, memory, disk, network, filesystem. Runs on each node, exposes `:9100/metrics`.

**9. Blackbox Exporter.** Probes endpoints over HTTP, HTTPS, TCP, ICMP, DNS to do synthetic uptime/latency checks. Returns probe success and duration as metrics.

**10. PromQL.** Prometheus Query Language — functional language for selecting and aggregating time-series data. Examples: `rate()`, `sum by(label)`, `histogram_quantile()`.

**11. Alertmanager.** Component that handles alerts sent by Prometheus: deduplication, grouping, silencing, routing to receivers (Slack, email, PagerDuty).

**12. Target.** An endpoint Prometheus scrapes (a host:port serving `/metrics`).

**13. Job.** A logical group of targets sharing the same purpose and config (e.g., job `node-exporter` with all node IPs).

**14. Default port.** Prometheus server: `9090`. Alertmanager: `9093`. Node Exporter: `9100`.

**15. Prometheus vs Grafana.** Prometheus collects+stores+alerts on metrics. Grafana visualizes data from Prometheus and other sources. They are complementary.

### Medium

**16. Prometheus architecture.**
Components: **Prometheus server** (retrieval + TSDB + PromQL + rule evaluator) → **Service Discovery** (file_sd, k8s_sd, consul_sd, ec2_sd) feeds targets → **Targets** expose `/metrics` → **Pushgateway** for short-lived jobs → **Alertmanager** receives firing alerts → **Receivers** (Slack/email/PagerDuty) → **Grafana** queries server for visualization.

**17. How metrics collected.** Prometheus pulls (HTTP GET) `/metrics` endpoints from configured targets at scrape interval. Targets expose metrics in plain-text exposition format.

**18. Pull-based monitoring.** Monitoring system fetches metrics from targets. Easier service discovery, target health visible (scrape failure = target down).

**19. Push-based monitoring.** Targets push metrics to monitoring system (StatsD, CloudWatch). Better for short-lived jobs and firewalled networks but harder to detect missing data.

**20. Why Prometheus uses pull.** Centralized control of scrape config, automatic up-metric for liveness, easier to test (curl `/metrics`), avoids overload from misbehaving clients.

**21. Pushgateway.** A push-accept intermediary; Prometheus scrapes the gateway. Used for batch/cron jobs whose lifetime is shorter than the scrape interval.

**22. When to use Pushgateway.** Only for service-level batch jobs (nightly ETL, backup). NOT for general apps — it breaks pull semantics and can hide crashes.

**23. Labels.** Key-value pairs attached to metrics enabling multi-dimensional slicing. Each unique label combination = unique time series.

**24. Cardinality.** Number of unique time series. High cardinality = many label combinations = high memory/CPU/disk on Prometheus.

**25. Why high cardinality is dangerous.** Memory blow-up (each series has overhead), slow queries, OOM, longer compactions. Avoid labels with unbounded values (user IDs, request IDs, full URLs).

**26. Counter, gauge, histogram, summary.**
- **Counter**: monotonic increasing (resets on restart) — `requests_total`.
- **Gauge**: arbitrary up/down — `memory_usage_bytes`.
- **Histogram**: pre-bucketed sample observations — `request_duration_seconds_bucket{le="0.1"}`.
- **Summary**: client-side computed quantiles — `request_duration_seconds{quantile="0.95"}`.

**27. Counter vs Gauge.** Counter only goes up (use `rate()` to derive per-second). Gauge can go up/down (current value).

**28. Histogram vs Summary.** Histograms are aggregatable across instances (use `histogram_quantile()` server-side), bucket boundaries fixed at config time. Summaries compute quantiles on the client (cheaper to query, but cannot aggregate across instances).

**29. Recording rules.** Pre-compute expensive PromQL expressions on a schedule and store as new series — speeds up dashboards and alerting on aggregations.

**30. Alerting rules.** PromQL expressions evaluated periodically; when true for `for:` duration, fire alert to Alertmanager.

**31. Prometheus + Kubernetes.** Via Prometheus Operator (CRDs: ServiceMonitor, PodMonitor, PrometheusRule). Operator generates scrape config from CRDs auto-discovered in the cluster.

**32. Service discovery in K8s.** `kubernetes_sd_configs` discovers pods/services/endpoints/nodes via the API server. Uses labels and annotations to filter scrape targets.

**33. Monitor CPU & memory.** Use Node Exporter metrics: `node_cpu_seconds_total`, `node_memory_MemAvailable_bytes`. For containers use cAdvisor: `container_cpu_usage_seconds_total`, `container_memory_working_set_bytes`. PromQL: `100 * (1 - avg by(instance)(rate(node_cpu_seconds_total{mode="idle"}[5m])))`.

**34. Configure targets.** In `prometheus.yml`:
```yaml
scrape_configs:
  - job_name: 'node'
    static_configs:
      - targets: ['10.0.0.1:9100','10.0.0.2:9100']
```
Or use service discovery (file_sd, kubernetes_sd, ec2_sd).

**35. ServiceMonitor.** A CRD from Prometheus Operator that declaratively defines how to scrape a Kubernetes Service. Operator translates into Prometheus scrape config.

### Difficult

**36. Scale Prometheus.**
Vertical (more CPU/RAM, SSD) up to a point; then **shard** by scrape config (one Prom per service group); use **Thanos** or **Cortex/Mimir** for global query view, long-term object-store retention, deduplication, and HA.

**37. Long-term storage.**
Local TSDB is bounded (default 15d). Use Thanos Sidecar to upload TSDB blocks to S3/GCS, Thanos Store for historical queries, Thanos Compactor for downsampling. Alternatives: Cortex, Mimir, VictoriaMetrics.

**38. Thanos.** Adds global query view, HA dedup, long-term object storage, downsampling on top of vanilla Prometheus. Components: Sidecar, Store Gateway, Querier, Compactor, Receive (for push).

**39. Cortex.** Multi-tenant, horizontally scalable Prometheus-compatible TSDB; ingest distributed across ingesters, chunks stored in object storage, queries via querier.

**40. VictoriaMetrics.** Drop-in Prometheus-compatible TSDB optimized for compression and ingest speed. Single-node and clustered modes; lower resource footprint than Thanos/Cortex.

**41. Avoid high cardinality.**
Don't use unbounded labels (user_id, request_id, full path with query string). Drop noisy labels via `metric_relabel_configs`. Use histograms with sensible bucket counts. Audit `topk(20, count by (__name__)({__name__=~".+"}))`.

**42. Alert for high error rate.**
```yaml
- alert: HighErrorRate
  expr: sum(rate(http_requests_total{status=~"5.."}[5m]))
        / sum(rate(http_requests_total[5m])) > 0.05
  for: 5m
  labels: { severity: critical }
  annotations:
    summary: "5xx error rate > 5% for 5m"
    runbook_url: "https://runbooks.example.com/high-error-rate"
```

**43. PromQL p95 latency.**
```promql
histogram_quantile(0.95,
  sum by (le, service) (rate(http_request_duration_seconds_bucket[5m]))
)
```

**44. Troubleshoot missing metrics.**
Check Prometheus **Targets** page (`/targets`) for `up` status and last scrape error. Curl the target's `/metrics` directly. Verify scrape config, network/firewall, service discovery labels, relabeling rules. Check Prometheus logs for parse errors.

**45. Prometheus server down.** No scraping/alerting during downtime; gap in metrics. Mitigation: HA pair with Alertmanager dedup, remote-write to a backup store, monitor Prometheus itself with a second instance.

**46. HA Prometheus.** Run two identical instances scraping the same targets. Both push to Alertmanager cluster (which dedups). Use Thanos/VM for unified query view.

**47. Monitor Prometheus itself.** Scrape its own `/metrics` (`prometheus_*` series) — TSDB head series, scrape duration, rule eval errors. Alert on `up{job="prometheus"} == 0` from a second instance.

**48. Optimize performance.** Use SSDs, tune `--storage.tsdb.retention`, drop unused labels via relabeling, use recording rules for heavy queries, shard or federate.

**49. Secure endpoints.** TLS on `/metrics` and Prometheus UI, basic-auth or OAuth proxy, network policies in K8s, scrape over HTTPS with bearer tokens (e.g., to kubelet).

**50. Alertmanager routing.**
```yaml
route:
  group_by: ['alertname','cluster']
  group_wait: 30s
  group_interval: 5m
  repeat_interval: 4h
  receiver: 'default'
  routes:
    - match: { severity: critical }
      receiver: 'pagerduty'
    - match: { team: payments }
      receiver: 'slack-payments'
```

**51. Silence alerts.** UI: Alertmanager → New Silence → matchers + duration + comment. CLI: `amtool silence add alertname=Foo --duration=2h`. Silence during planned maintenance windows.

**52. Avoid duplicate alerts in HA.** Run Alertmanager as a cluster (gossip via `--cluster.peer`); configure Prometheus to send to all AM instances. AM cluster dedups based on alert fingerprints.

---

## SECTION 4 — GRAFANA

### Easy

**1. Grafana.** Open-source observability visualization platform; queries multiple data sources and renders interactive dashboards/alerts.

**2. Why used.** Unified view across Prometheus, Elasticsearch, CloudWatch, Loki, Tempo, etc.; rich visualizations; templating; sharable dashboards; alerting.

**3. Dashboard.** A collection of panels grouped to display a system's state. Saved as JSON, version-controllable.

**4. Panel.** A single visualization (graph, stat, table, gauge, heatmap) backed by one or more queries.

**5. Data source.** A connection to a backend system (Prometheus URL, ES cluster, CloudWatch account) Grafana queries.

**6. Sources Grafana connects to.** Prometheus, Elasticsearch, Loki, Tempo, InfluxDB, MySQL, PostgreSQL, CloudWatch, Azure Monitor, Stackdriver, Graphite, Jaeger, Zipkin, Splunk, OpenSearch — 60+ via plugins.

**7. Prometheus data source.** Yes — native first-class support; PromQL editor with autocomplete.

**8. Elasticsearch data source.** Yes — native; Lucene/KQL queries; supports logs and metrics.

**9. Variable.** A reusable placeholder (`$cluster`, `$namespace`) populated from a query/list; lets one dashboard render for many entities.

**10. Row.** A horizontal grouping of panels in a dashboard, can be collapsed.

**11. Annotation.** A timestamped marker on a graph (deploys, incidents) sourced from a query or manually.

**12. Grafana alerting.** Native unified alerting (since v8) — alert rules evaluated by Grafana itself, routed via contact points and notification policies. Replaces legacy panel alerts.

### Medium

**13. Create a dashboard.** + → Dashboard → Add panel → choose data source → write query → choose visualization → set thresholds, units, legends → save.

**14. Add Prometheus data source.** Configuration → Data Sources → Add → Prometheus → URL `http://prometheus:9090` → Save & Test.

**15. Add Elasticsearch.** Configuration → Data Sources → Elasticsearch → URL, index pattern (`logs-*`), time field (`@timestamp`), version → Save & Test.

**16. CPU panel.** PromQL: `100 - (avg by(instance)(rate(node_cpu_seconds_total{mode="idle"}[5m])) * 100)`. Visualization: Time series, unit %, threshold red @ 85.

**17. Memory & disk dashboards.** Memory: `(1 - node_memory_MemAvailable_bytes / node_memory_MemTotal_bytes) * 100`. Disk: `100 - (node_filesystem_avail_bytes{mountpoint="/"} / node_filesystem_size_bytes{mountpoint="/"} * 100)`.

**18. Kubernetes dashboard.** Use community dashboards (e.g., 315, 6417, kube-state-metrics). Or build from `kube_pod_status_phase`, `kube_deployment_status_replicas`, `container_cpu_usage_seconds_total`.

**19. Variables.** Dashboard settings → Variables → Add → type `Query`, data source Prometheus, query `label_values(kube_pod_info, namespace)` → use as `$namespace` in panel queries.

**20. Dynamic dashboards.** Use multi-value variables (`$service`, `$instance`) and chained variables (cluster → namespace → pod). Panel repeat by variable for one-panel-per-instance.

**21. Thresholds.** Panel → Field → Thresholds → set values + colors (e.g., green < 70, yellow 70–85, red > 85). Use in stat/gauge for clear visual.

**22. Configure alerts.** Alerting → Alert Rules → New rule → query + condition (`is above 0.05 for 5m`) → labels + annotations → notification policy → contact point.

**23. Share dashboard.** Share icon → Link / Snapshot / Export JSON / Embed iframe. Snapshots are anonymized public copies.

**24. Import / export.** Export: Dashboard settings → JSON Model. Import: + → Import → upload JSON or paste grafana.com dashboard ID.

**25. Permissions.** Folder/dashboard ACLs: Viewer / Editor / Admin per user/team. Org-level roles. Use folders to group dashboards by team.

**26. Folders.** Logical grouping container for dashboards; permissions can be inherited from folder.

**27. Grafana alerting vs Alertmanager.** Grafana evaluates alerts at the visualization layer across multiple data sources. Alertmanager handles alerts from Prometheus only and is purpose-built for routing/dedup/grouping. Grafana 8+ unified alerting can integrate with Alertmanager.

### Difficult

**28. Enterprise structure.** Folders per team/domain → dashboards per service. Use a "Home" overview folder with org-wide health. Standardize naming, tags, variable conventions. Manage as code via provisioning + Git.

**29. Dashboard performance.** Reduce queries per panel, avoid `*` regex matchers, use recording rules, lower refresh interval, time-range cap, limit cardinality of variables, use `Min step`.

**30. "No data" troubleshooting.** Check data source health (Save & Test), time range, query syntax, target up status in Prometheus, label filters matching nothing, variable resolution (Inspect → Query).

**31. Secure access.** TLS, auth provider (LDAP, OAuth, SAML, OIDC), org-based isolation, anonymous off, API key rotation, RBAC roles, audit log enabled.

**32. LDAP/SSO.** Configure in `grafana.ini` `[auth.ldap]` with `ldap.toml` for server + group mapping. SAML/OIDC similar via `[auth.saml]` / `[auth.generic_oauth]`. Map IdP groups to Grafana roles.

**33. Multi-cluster K8s dashboards.** Add `cluster` label via Prometheus external_labels. Use cluster as a Grafana variable. One dashboard renders per cluster selection.

**34. Dashboards as code.** Version JSON in Git. Render via Jsonnet (grafonnet) or Terraform `grafana_dashboard` resource. Provision via ConfigMap + sidecar in Kubernetes.

**35. Provisioning.** Files in `/etc/grafana/provisioning/` — `datasources/`, `dashboards/`, `notifiers/`, `alerting/`. Grafana auto-loads on startup. Source-controlled, no UI clicks.

**36. Terraform-managed dashboards.**
```hcl
resource "grafana_dashboard" "rd" {
  config_json = file("dashboards/red.json")
  folder      = grafana_folder.team.id
}
```
Use `grafana_data_source`, `grafana_folder`, `grafana_alert_rule` for full IaC.

**37. Latency/error/throughput panels.** RED method — three panels: rate (`sum(rate(req_total[5m]))`), error% (`sum(rate(req_total{status=~"5.."}[5m])) / sum(rate(req_total[5m]))`), p95 latency (`histogram_quantile(0.95, sum by(le)(rate(req_duration_bucket[5m])))`).

**38. Executive dashboards.** Single screen, large stat panels for SLA %, MTTR, incident count, regional uptime. Sparkline trend, no PromQL exposed. Auto-refresh 5m. No drill-down.

**39. Reduce clutter.** One dashboard = one purpose. ≤12 panels per dashboard. Remove duplicates. Use variables instead of repeated panels. Archive unused dashboards.

**40. Decide important panels.** Tied to SLOs and on-call decisions. If a panel can't answer "is my service healthy?" or "what's wrong?", drop it.

---

## SECTION 5 — ELK STACK

### Easy

**1. ELK.** Elasticsearch + Logstash + Kibana — open-source log aggregation/search/visualization stack from Elastic.

**2. Elasticsearch.** Distributed search and analytics engine built on Apache Lucene. Stores JSON documents in indices, supports near-real-time search and aggregation.

**3. Logstash.** Server-side data processing pipeline — ingests, parses, transforms, and forwards data to Elasticsearch. Plugin-based: input, filter, output.

**4. Kibana.** Web UI for Elasticsearch — search/explore (Discover), dashboards, visualizations, alerting, dev tools.

**5. Why ELK.** Centralized logging, full-text search across petabytes, near-real-time log analysis, visualization, alerting, audit trail.

**6. Filebeat.** Lightweight Go-based log shipper (Beats family); tails files, ships to Logstash or Elasticsearch directly. Replaces heavy Logstash on edge nodes.

**7. Index.** Logical namespace for documents (like a DB table). E.g., `logs-app-2026.04.28`.

**8. Document.** A single JSON record stored in an index — one log line, one event.

**9. Field.** A key-value pair inside a document with a type (text, keyword, date, integer).

**10. Log aggregation.** Centralizing logs from many sources into one searchable system.

**11. Kibana use.** Search logs, build dashboards, set alerts, manage Elasticsearch (snapshots, ILM, security), run Dev Tools queries.

**12. KQL.** Kibana Query Language — simple syntax for filtering: `status:500 and service:"checkout" and message:*timeout*`.

### Medium

**13. ELK architecture.**
App/host → **Filebeat** (tail logs) → **Logstash** (parse/enrich/filter via grok, GeoIP, mutate) → **Elasticsearch** (index + store) → **Kibana** (query + visualize).
For high volume, insert **Kafka** between Filebeat and Logstash as a buffer.

**14. Log flow.** App writes to file → Filebeat reads → ships to Logstash via Beats input → Logstash parses (grok), enriches → bulk-indexes to ES → Kibana index pattern → user searches.

**15. Filebeat role.** Tail log files, track offset (registry), ship lines to Logstash/Kafka/ES with backpressure-aware delivery.

**16. Logstash role.** Heavy parsing, transformation, enrichment, multi-output routing. Pipeline = input → filter → output.

**17. ES storage.** Documents broken into shards, indexed via inverted index, written to segments on disk; replicated across nodes for HA. Translog for durability.

**18. Index pattern.** Kibana abstraction matching one or more ES indices (e.g., `logs-*`) for Discover/dashboards.

**19. Shard.** A horizontal partition of an index — a self-contained Lucene index. Index can have N primary shards.

**20. Replica.** A copy of a shard on another node — provides HA and read scaling.

**21. Mapping.** Schema for an index — declares field types and analyzers. Dynamic by default; explicit mappings recommended for production.

**22. Ingest pipeline.** ES-side processing (lighter than Logstash) — chain of processors (grok, set, remove, date) applied at index time.

**23. Parse logs in Logstash.**
```ruby
filter {
  grok { match => { "message" => "%{TIMESTAMP_ISO8601:ts} %{LOGLEVEL:level} %{GREEDYDATA:msg}" } }
  date { match => ["ts","ISO8601"] target => "@timestamp" }
}
```

**24. Grok patterns.** Regex named patterns for parsing unstructured text into fields. Library has `IP`, `WORD`, `NUMBER`, `TIMESTAMP_ISO8601`, etc. Custom: `(?<field>regex)`.

**25. Search error logs.** Kibana Discover, KQL `level:ERROR and service:"payments"`, time range last 1h, save as a search.

**26. Kibana dashboards.** Create visualizations (Lens or Aggregation-based) → add to dashboard → set time range and filters → save.

**27. Log retention.** Use Index Lifecycle Management (ILM) — Hot → Warm → Cold → Delete tiers based on age/size. E.g., delete logs > 30d.

**28. ILM.** Index Lifecycle Management — automated phase transitions for indices: rollover when size/age threshold hit, move to cheaper hardware, delete old data.

**29. Cluster health.** GET `_cluster/health` → status green/yellow/red. Green = all primaries+replicas assigned. Yellow = primaries OK, replicas missing. Red = primaries unassigned (data unavailable).

### Difficult

**30. Missing logs in Kibana.** Check Filebeat status & registry, Logstash queue/pipeline lag, ES cluster health, index pattern match, time range, Logstash drop filter, parse failures (`tags:_grokparsefailure`), ILM deletion.

**31. Yellow/red cluster.** Yellow → unassigned replicas; check disk watermark, node count, allocation explain (`GET _cluster/allocation/explain`). Red → primaries unassigned; restore from snapshot or fix allocation issue. Check `disk.watermark.low/high/flood_stage`.

**32. Optimize ES.** Right-size shards (30–50 GB), reduce mapping fields, disable `_all`, use `keyword` not `text` where no full-text needed, force-merge cold indices, tune JVM heap (≤32 GB).

**33. High JVM memory.** Heap ≥ 75% sustained → fielddata, large aggregations, oversharding, mapping explosion. Check `_cat/fielddata`, hot threads. Increase heap (≤32 GB), reduce shards, use `keyword` for sorts.

**34. Large log volume.** Add Kafka buffer; horizontally scale Logstash; rollover indices; use ILM hot/warm/cold; downsample; move noisy logs to S3 instead of ES.

**35. Design ILM.** Hot 7d (SSD, indexing) → Warm 30d (HDD, read-only, force-merge) → Cold 90d (frozen tier or S3 searchable snapshot) → Delete. Use rollover by size (50 GB) and age (1d).

**36. Secure ELK.** X-Pack Security: TLS between nodes & clients, RBAC, role-based field/document-level security, API key auth, audit log, IP filtering, network isolation.

**37. Control ingestion cost.** Drop noisy log levels (DEBUG/INFO) at Filebeat/Logstash, sample, route to S3 for archival, ILM aggressive deletion, exclude health-check logs.

**38. Avoid duplicate logs.** Idempotent document IDs (`fingerprint` filter on message hash + timestamp), Filebeat registry persistence, single-source-of-truth shipping.

**39. Parse unstructured logs.** Grok with custom patterns, dissect for fixed-position parsing, `kv` filter for key=value, JSON filter where possible. Encourage app-side structured JSON logging.

**40. Multiline logs.** Filebeat `multiline.pattern` config (e.g., `^[[:space:]]+|^Caused by:` for Java stacktraces) with `multiline.match: after`.

**41. Kubernetes pod logs.** Fluent Bit / Filebeat DaemonSet tails `/var/log/containers/*.log`, adds k8s metadata (namespace, pod, labels), ships to ES.

**42. Centralized logging design.** Edge collector (Fluent Bit/Filebeat) → Kafka buffer → Logstash/Fluentd processors → ES (multi-tier ILM) → Kibana. Multi-tenant via index per team + RBAC.

**43. Correlate logs with trace IDs.** Inject `trace_id`/`span_id` into log MDC at app side → Filebeat ships → ES indexes → Kibana link panel jumps to trace UI (Jaeger/Tempo) for that ID.

---

## SECTION 6 — EFK STACK

### Easy

**1. EFK.** Elasticsearch + **Fluentd** (or Fluent Bit) + Kibana. Same as ELK but Fluentd replaces Logstash.

**2. ELK vs EFK.** Logstash is JVM-heavy; Fluentd/Fluent Bit are lighter (Ruby/C), better suited to Kubernetes DaemonSet deployment. Same ES + Kibana backend.

**3. Fluentd.** CNCF data collector written in Ruby (with C core), unified logging layer, plugin ecosystem.

**4. Fluent Bit.** Lightweight (C, ~hundreds of KB) sibling of Fluentd — designed for edge/embedded/Kubernetes deployments.

**5. EFK in Kubernetes.** Fluent Bit runs as DaemonSet (one per node), tails container logs, enriches with k8s metadata, ships to ES. Lightweight, minimal resource footprint.

**6. ES role in EFK.** Same — store and search logs.

**7. Kibana role in EFK.** Same — query and visualize.

### Medium

**8. EFK architecture in K8s.**
Fluent Bit DaemonSet on each node → tails `/var/log/containers/*.log` → enriches via `kubernetes_metadata` filter (pod, namespace, labels) → forwards to Fluentd aggregator (optional) or directly to ES → Kibana dashboards.

**9. How Fluentd collects pod logs.** Tails container log files on host nodes. Container runtime writes stdout/stderr to `/var/log/containers/`. `tail` input reads lines and parses JSON.

**10. Fluent Bit vs Fluentd.** Fluent Bit: C, ~450 KB, low CPU/RAM, fewer plugins, ideal for edge. Fluentd: Ruby, larger, more plugins, better for aggregation/processing layer.

**11. DaemonSet.** Kubernetes workload that runs one pod on every node (or selected nodes). Used for node-level agents — log shippers, monitoring, networking.

**12. Why Fluent Bit as DaemonSet.** Each node has unique log files; needs one collector per node to read them. DaemonSet guarantees that placement.

**13. Container log location.** `/var/log/containers/*.log` (symlinks to `/var/log/pods/<pod-uid>/<container>/0.log`). Container runtime (containerd/CRI-O) writes there.

**14. Pod metadata enrichment.** `kubernetes` filter calls API server (or reads from local API) to fetch pod labels, namespace, container name, node, and adds them as fields.

**15. Filter logs before ES.** Fluent Bit `[FILTER]` blocks: `grep` to drop, `modify` to add/remove fields, `lua` for custom logic. Fluentd `<filter>` blocks similar.

**16. Troubleshoot Fluentd/Fluent Bit.** Check pod logs (`kubectl logs ds/fluent-bit`), `/api/v1/metrics` endpoint (Fluent Bit exposes Prometheus metrics), buffer state on disk, ES connection health, parse failures.

### Difficult

**17. EFK for multi-cluster K8s.** Per-cluster Fluent Bit DaemonSet → regional Fluentd aggregator → cross-region ES (or per-region ES with cross-cluster search in Kibana). Add `cluster` label for filtering.

**18. High log volume.** Aggregator tier (Fluentd) with disk buffer + Kafka in front of ES; horizontally scale; ILM rollover; backpressure-aware shippers.

**19. Avoid ES overload.** Rate-limit at Fluent Bit (`Mem_Buf_Limit`, `storage.type filesystem`), use Kafka as buffer, drop noisy logs, increase ES bulk threads, scale ES ingest nodes.

**20. Log retention in EFK.** ES ILM policies (hot/warm/cold/delete), snapshot to S3 for archival, age-based index deletion via Curator/ILM.

**21. Secure EFK.** TLS between Fluent Bit and ES, ES X-Pack RBAC, API keys, network policies in K8s, restrict who reads which index (field-level security).

**22. Mask sensitive data.** Fluent Bit `modify` or `lua` filter to redact (regex replace credit cards, emails); ingest pipelines on ES side with `gsub`. App-side scrubbing is best.

**23. Debug missing pod logs.** Check FB pod is running on the node, is the file present, exclude rules, parser errors, output destination reachable, ES backpressure (buffer full).

**24. Multiline Java stack traces.** Fluent Bit `multiline.parser` (e.g., `java`) joins continuation lines (lines starting with whitespace or `Caused by:`) into one event.

**25. Route by namespace.** Use `kubernetes` filter to add namespace, then `[OUTPUT]` with `Match kube.var.log.containers.*_<ns>_*` and `index ns-<ns>-logs-%Y.%m.%d`. Or use record_modifier + multi-output.

**26. Optimize Fluent Bit resources.** Tune `Mem_Buf_Limit`, `Flush` interval, disable verbose logging, reduce filter chain, use `storage.type filesystem` to spill instead of dropping.

---

## SECTION 7 — KUBERNETES

### Easy

**1. Kubernetes.** Open-source container orchestration platform from Google (donated to CNCF). Automates deployment, scaling, healing of containerized workloads.

**2. Why used.** Declarative deployments, self-healing, horizontal scaling, service discovery, rolling updates, multi-cloud portability.

**3. Pod.** Smallest deployable unit; one or more containers sharing network namespace and storage volumes. Ephemeral.

**4. Node.** A worker machine (VM or bare metal) running pods. Has kubelet, kube-proxy, container runtime.

**5. Cluster.** Control plane + nodes managed as one unit.

**6. Deployment.** Declarative manager for stateless apps — handles rolling updates, rollbacks, ReplicaSets.

**7. Service.** Stable network endpoint (cluster IP + DNS) load-balancing to a set of pods selected by labels.

**8. Namespace.** Logical partition of cluster resources for multi-tenancy and isolation.

**9. ReplicaSet.** Ensures N copies of a pod are running; managed by a Deployment.

**10. ConfigMap.** Key-value config injected into pods as env vars or files. For non-secret configuration.

**11. Secret.** Same as ConfigMap but base64-encoded (not encrypted by default unless KMS-enabled etcd) for sensitive data.

**12. Ingress.** API object managing external HTTP(S) access to services — host/path routing, TLS termination. Implemented by an Ingress controller (nginx, ALB, Traefik).

**13. kubeconfig.** YAML file describing clusters, users, contexts; used by `kubectl` to authenticate.

**14. kubectl.** CLI tool talking to Kubernetes API server.

**15. Container runtime.** Software that runs containers — containerd, CRI-O. (Docker shim removed since 1.24.)

**16. YAML in K8s.** Declarative manifest format for objects (Deployment, Service, ConfigMap).

**17. Helm.** Kubernetes package manager — bundles manifests as **charts** with templated values; `helm install/upgrade/rollback`.

### Medium

**18. K8s architecture.** *(See Section A Q5.)* Control plane + worker nodes; declarative reconciliation loop.

**19. Control plane components.** API Server, etcd, Scheduler, Controller Manager, Cloud Controller Manager.

**20. API Server.** Front-end REST API for the cluster; validates and persists objects to etcd; gateway for all other components.

**21. etcd.** Distributed key-value store holding cluster state. Strongly consistent (Raft). Backup is critical.

**22. Scheduler.** Watches unscheduled pods, picks a node based on resource requests, taints/tolerations, affinity, topology, and binds pod to node.

**23. Controller Manager.** Runs reconciliation loops for built-in controllers — Deployment, ReplicaSet, Node, Endpoints, Job, ServiceAccount.

**24. kubelet.** Node-level agent; receives pod specs from API server, instructs container runtime, reports node/pod status, runs probes.

**25. kube-proxy.** Maintains network rules (iptables/IPVS/eBPF) on each node implementing Service load balancing.

**26. Deployment vs StatefulSet.** Deployment: stateless, interchangeable pods, random names. StatefulSet: stable network ID (`pod-0`), stable storage (PVC per pod), ordered rollout. Use StatefulSet for DBs, Kafka, ZooKeeper.

**27. ClusterIP / NodePort / LoadBalancer.** ClusterIP: internal only. NodePort: opens a port on every node (30000–32767). LoadBalancer: provisions a cloud LB (AWS NLB/ALB) routing to NodePorts.

**28. ConfigMap vs Secret.** Same shape; Secret base64-encoded, may be encrypted at rest (with KMS provider), and access-restricted via RBAC. Use Secret for credentials, certs, tokens.

**29. ReplicaSet vs Deployment.** ReplicaSet ensures pod count. Deployment manages ReplicaSets to provide rolling updates and rollback.

**30. Pod vs container.** Pod is a wrapper around one or more containers sharing network/IPC/storage. Containers run the actual processes.

**31. Labels & selectors.** Labels are key=value tags on objects. Selectors filter objects by labels (`app=nginx`). Services use selectors to find target pods.

**32. Taints & tolerations.** Taint repels pods from a node (`kubectl taint node n1 gpu=true:NoSchedule`); pods need a matching Toleration to land there. Used to dedicate nodes.

**33. Node selectors.** Simple `nodeSelector: { disktype: ssd }` field on pod spec to constrain to nodes with matching label.

**34. Affinity / anti-affinity.** Richer scheduling: `requiredDuringScheduling…` or `preferredDuringScheduling…` based on node labels (nodeAffinity) or other pod labels (podAffinity / podAntiAffinity).

**35. Resource requests & limits.** Request: minimum guaranteed (used by scheduler). Limit: maximum allowed (enforced by kubelet/cgroups). CPU throttled, memory OOMKilled.

**36. HPA.** Horizontal Pod Autoscaler — scales replicas based on CPU/memory or custom metrics (Prometheus adapter). `kubectl autoscale deployment app --min=2 --max=10 --cpu-percent=70`.

**37. Rolling update.** Deployment strategy that replaces pods incrementally (`maxSurge`, `maxUnavailable`), zero downtime.

**38. Rollback.** `kubectl rollout undo deployment/app` reverts to the previous ReplicaSet.

**39. Liveness probe.** Periodic check (HTTP/TCP/exec); failure → kubelet restarts the container. Use for deadlock detection.

**40. Readiness probe.** Failure → pod removed from Service endpoints (no traffic) but not restarted. Use for warm-up / dependency checks.

**41. Startup probe.** Replaces liveness during slow startup; once successful, liveness takes over. Avoids killing slow-booting apps.

**42. Persistent Volume (PV).** Cluster-wide storage resource — disk, NFS, EBS — provisioned by admin or dynamically.

**43. Persistent Volume Claim (PVC).** User's request for storage (size, access mode); binds to a PV.

**44. StorageClass.** Defines a "class" of storage with a provisioner (e.g., `ebs.csi.aws.com`) and parameters; enables dynamic PV provisioning.

**45. Service discovery.** CoreDNS resolves `service.namespace.svc.cluster.local` to ClusterIP. kube-proxy iptables/IPVS rules forward to backend pods.

### Difficult

**46. CrashLoopBackOff.** `kubectl describe pod` for events; `kubectl logs <pod> -p` for previous container; check OOMKilled (memory limits), failing liveness, missing config/secret, app exception, image entrypoint error.

**47. ImagePullBackOff.** `describe` for error: missing image, wrong tag, registry auth (imagePullSecret), no network to registry, rate limit. `kubectl get secrets`, ensure correct registry credential.

**48. Pod Pending.** No node fits — insufficient CPU/memory, taints without toleration, PVC unbound, node selector unmatched. `kubectl describe pod` events show scheduler reason.

**49. Node NotReady.** SSH to node; check kubelet (`systemctl status kubelet`), disk pressure, network plugin (CNI), kube-proxy. `kubectl describe node` shows conditions.

**50. Service connectivity.** Endpoints populated? (`kubectl get endpoints svc`). Pod labels match selector? Probe ready? NetworkPolicy blocking? kube-proxy mode? DNS resolution?

**51. DNS issues.** CoreDNS pods running? Check `/etc/resolv.conf` in pod, run `nslookup kubernetes.default` from a debug pod, check ndots, CoreDNS ConfigMap.

**52. High CPU/memory in pods.** `kubectl top pod`, profile app, check requests/limits, GC behavior, leak. Use Prometheus + Grafana for trend.

**53. App works in pod but not via service.** Probes failing → pod not in endpoints. Check `kubectl get endpoints svc`, readiness probe, port mismatch, NetworkPolicy.

**54. Monitor cluster.** kube-state-metrics + Node Exporter + Prometheus + Grafana + Alertmanager. Optional Pixie/Cilium Hubble for network observability.

**55. Collect K8s metrics.** kubelet `/metrics/cadvisor` for container metrics, `/metrics/resource` for resource usage, kube-state-metrics for object state, API server metrics for control plane.

**56. Collect K8s logs.** Fluent Bit/Fluentd DaemonSet → ES/Loki/CloudWatch. Stdout/stderr from containers go to node disk; collector ships them.

**57. Secure secrets.** Enable encryption-at-rest with KMS provider in API server config; restrict RBAC to `secrets`; use external Secret Stores (Vault, AWS Secrets Manager) via External Secrets Operator or CSI driver; audit access.

**58. RBAC.** Role/ClusterRole defines verbs+resources; RoleBinding/ClusterRoleBinding ties them to subjects (User, Group, ServiceAccount). Default-deny model.

**59. Restrict access.** RBAC roles, namespace isolation, NetworkPolicies, PodSecurityAdmission, Quotas, OPA/Gatekeeper policies.

**60. Cluster upgrade.** Drain node → upgrade kubeadm/kubelet → upgrade control plane (managed by EKS/AKS/GKE control plane upgrade), then rolling node-group replacement. Test in non-prod first; verify CRDs/API versions.

**61. Backup/restore etcd.** `etcdctl snapshot save snapshot.db` (with TLS certs) → store in S3. Restore: `etcdctl snapshot restore` to recreate data dir, point new etcd member at it. Schedule daily backups.

**62. HA workloads.** Multiple replicas, PodDisruptionBudget, anti-affinity across nodes/zones, readiness probes, multi-AZ node groups, HPA, rolling updates.

**63. PodDisruptionBudget.** Limits voluntary disruptions: `minAvailable: 2` ensures at least 2 pods always running during drains/upgrades.

**64. NetworkPolicy.** Pod-level firewall — restrict ingress/egress by pod selectors, namespaces, IP blocks. Requires CNI that supports it (Calico, Cilium).

**65. Expose apps securely.** Ingress with TLS (cert-manager + Let's Encrypt), WAF in front (AWS WAF/Cloudflare), authentication at ingress (OIDC), NetworkPolicies, mTLS via service mesh.

**66. Monitor with Prometheus + Grafana.** kube-prometheus-stack (Helm chart) bundles Prometheus Operator + kube-state-metrics + Node Exporter + Alertmanager + Grafana with default dashboards.

**67. Design observability for K8s.** kube-prometheus-stack for metrics, Fluent Bit/Loki for logs, Tempo/Jaeger for traces, OTel Collector for unified pipeline, Grafana for visualization, Alertmanager → PagerDuty/Slack.

**68. Failed deployments.** `kubectl rollout status deployment/app`; `kubectl describe deployment` for events; `kubectl get rs` for new ReplicaSet state; `kubectl logs` for pod errors. Rollback with `rollout undo`.

**69. Check events.** `kubectl get events -n <ns> --sort-by=.lastTimestamp` (or `--watch`). Events live ~1h.

**70. Troubleshooting commands.** `kubectl get pods -o wide`, `describe`, `logs -p`, `exec -it`, `top`, `events`, `get endpoints`, `rollout`, `port-forward`, `auth can-i`, `cluster-info dump`.

---

## SECTION 8 — KUBECONFIG

### Easy

**1. kubeconfig.** YAML file telling `kubectl` how to authenticate to clusters.

**2. Why needed.** Stores API server URL, CA cert, user credentials, namespace context — without it kubectl can't talk to a cluster.

**3. Where stored.** Default `~/.kube/config` (Linux/macOS), `%USERPROFILE%\.kube\config` (Windows). Override via `--kubeconfig` flag or `$KUBECONFIG` env var.

**4. Default path.** `~/.kube/config`.

**5. Context.** Named tuple of (cluster + user + namespace) — switch to change which cluster you're targeting.

**6. Cluster entry.** `name`, `server` (API URL), `certificate-authority-data` (CA cert).

**7. User entry.** `name` + auth method (`client-certificate`, `token`, `exec` plugin, `username/password`).

**8. Namespace.** Default namespace for the context; `kubectl` commands without `-n` use this.

### Medium

**9. Switch context.** `kubectl config use-context dev-cluster`. Or `kubectx dev-cluster` (helper tool).

**10. Check current context.** `kubectl config current-context`. View all: `kubectl config get-contexts`.

**11. Add new cluster.**
```
kubectl config set-cluster prod --server=https://... --certificate-authority=ca.crt
kubectl config set-credentials admin --token=...
kubectl config set-context prod-ctx --cluster=prod --user=admin --namespace=default
kubectl config use-context prod-ctx
```

**12. Authentication flow.** kubectl reads kubeconfig → presents credential to API server → API server validates (cert, token, OIDC, webhook) → maps to user/groups → RBAC authorizes the request.

**13. Certificate-based auth.** Client cert signed by cluster CA in user's `client-certificate-data` and `client-key-data` fields. Mutual TLS at API server.

**14. Token-based auth.** Bearer token (ServiceAccount token, OIDC token, static token) in `token` or fetched via `exec` plugin (e.g., `aws eks get-token`).

**15. EKS kubeconfig.** `aws eks update-kubeconfig --name cluster --region us-east-1` writes a context using `exec` block calling `aws eks get-token` for ephemeral creds tied to IAM.

**16. Merge kubeconfigs.** `KUBECONFIG=~/.kube/config:~/.kube/dev:~/.kube/prod kubectl config view --merge --flatten > ~/.kube/merged && mv ~/.kube/merged ~/.kube/config`.

**17. Permission errors.** Decode the user mapping (`kubectl auth whoami`), check RBAC: `kubectl auth can-i list pods -n ns`. Inspect `RoleBinding`/`ClusterRoleBinding`. For EKS, check `aws-auth` ConfigMap.

### Difficult

**18. Securely manage.** Don't commit to Git; use chmod 600; rotate creds; use short-lived tokens (OIDC, EKS get-token); store via secret manager and inject in CI; isolate per-environment kubeconfig.

**19. If leaked.** Anyone with the file can act as that user → revoke immediately: rotate cert/CA, invalidate tokens, remove user from `aws-auth`/IdP, audit recent activity (CloudTrail, audit log), regenerate kubeconfig.

**20. Limited access via kubeconfig.** Issue per-user ServiceAccount with narrow Role (read-only on a namespace) → generate kubeconfig with that SA token. Or OIDC-mapped group with restricted ClusterRoleBinding.

**21. Rotate creds.** Issue new client cert / token → update kubeconfig → distribute → revoke old after grace period. For EKS, the exec-based token rotates automatically; rotate the underlying IAM credentials.

**22. RBAC interaction.** Authentication identifies *who*; RBAC decides *what they can do*. `aws-auth` ConfigMap on EKS maps IAM to K8s users/groups, then standard RBAC applies.

**23. "You must be logged in to the server".** Token expired/invalid, exec plugin failure, wrong cluster CA. Run `aws eks get-token` manually to validate; check IAM permissions on the user.

**24. "Unable to connect to the server".** Network/DNS to API server endpoint, wrong server URL, API server down, VPN/firewall, expired CA. Try `curl -k https://<server>/healthz`.

**25. Jenkins kubeconfig.** Store as `Secret file` credential in Jenkins, inject in pipeline:
```groovy
withCredentials([file(credentialsId:'kubeconfig', variable:'KUBECONFIG')]) {
  sh 'kubectl apply -f deploy.yaml'
}
```
Or use Jenkins Kubernetes plugin with in-cluster ServiceAccount (no kubeconfig needed).

---

## SECTION 9 — OPENTELEMETRY

### Easy

**1. OpenTelemetry (OTel).** CNCF observability framework providing vendor-neutral APIs, SDKs, and a Collector for generating, collecting, and exporting telemetry (metrics, logs, traces).

**2. Why used.** Standardizes instrumentation across languages/vendors; one SDK, many backends; avoids lock-in; supported by all major APM vendors.

**3. Telemetry signals.** Metrics, Logs, Traces (and emerging: Events, Profiles).

**4. Metrics.** Numeric measurements; OTel supports counters, gauges, histograms (and ExponentialHistograms).

**5. Logs.** Structured log records with correlation IDs (trace_id, span_id) for cross-signal linking.

**6. Traces.** Request flow as a tree of spans across services; W3C Trace Context propagation.

**7. Span.** A single unit of work in a trace — name, start/end, attributes, events, status.

**8. Trace.** Root span + its descendants — entire request flow.

**9. Context propagation.** Trace context (trace_id, span_id, flags) carried across process boundaries via headers (W3C `traceparent`/`tracestate`).

**10. OTel Collector.** Vendor-agnostic agent/gateway that receives, processes, and exports telemetry. Decouples app from backend.

### Medium

**11. OTel architecture.**
App SDK → exports OTLP → **Collector** (receivers → processors → exporters) → Backend (Prometheus/Jaeger/New Relic/etc). Collector can be deployed as agent (sidecar/DaemonSet) and gateway (central).

**12. Instrumentation.** Code that emits telemetry. Two flavors: auto (zero-code) and manual.

**13. Auto-instrumentation.** Bytecode/runtime injection (Java agent, Python `opentelemetry-instrument`, Node.js loader) instruments common libs (HTTP, DB drivers) without code changes.

**14. Manual instrumentation.** Developer adds spans/metrics in code via OTel API for custom logic.

**15. Receivers.** Collector inputs — OTLP, Jaeger, Zipkin, Prometheus scrape, Kafka, Filelog.

**16. Processors.** Pipeline transformations — batch, memory_limiter, attribute, filter, tail_sampling, k8sattributes, resource detection.

**17. Exporters.** Collector outputs — OTLP, Prometheus remote write, Elasticsearch, Loki, Jaeger, New Relic, Datadog, AWS X-Ray, S3.

**18. OTLP.** OpenTelemetry Protocol — gRPC/HTTP wire format for sending telemetry. Native protocol of OTel.

**19. OTel + Prometheus.** Use Prometheus receiver (scrape) + Prometheus remote write exporter; or expose collector's `/metrics` endpoint for Prometheus to scrape.

**20. OTel + Jaeger.** Configure OTLP or Jaeger exporter in Collector; Jaeger backend ingests spans for visualization.

**21. OTel + New Relic.** OTLP exporter → New Relic OTLP endpoint with license key as auth header. NR ingests metrics+logs+traces.

**22. Helps in microservices.** Standardized cross-service trace context, shared APIs, correlate logs/traces, swap backends without re-instrumenting.

**23. Traces in troubleshooting.** Show end-to-end request flow → identify which span/service has highest latency or error → drill into that span's attributes/logs.

**24. Distributed tracing.** Tracing a single request across many services using a shared trace_id, propagated via HTTP headers / message metadata.

**25. Trace ID in logs.** Add `trace_id`/`span_id` to MDC/log context; appears in JSON log lines; ES/Loki indexes them for cross-signal correlation.

### Difficult

**26. Implement in microservices.** Add OTel SDK + auto-instrumentation per language; central OTel Collector (DaemonSet + gateway); standardize resource attributes (`service.name`, `deployment.environment`); use W3C propagation; configure exporters per backend.

**27. Missing traces.** Check SDK config (endpoint, auth), sampling rate, processor batch dropping, Collector logs, exporter errors, network/firewall, backend ingest limits.

**28. Reduce trace volume.** Sampling (head/tail), drop noisy spans (health checks), filter at Collector with `filter` processor, downsample slow paths.

**29. Sampling.** Decision to record/export only a fraction of traces. Reduces cost while preserving visibility.

**30. Head-based vs tail-based sampling.** **Head-based**: decision at start of trace (random, deterministic) — simple, cheap, may miss errors. **Tail-based**: decision after full trace seen (in Collector) — keeps errored/slow traces, more memory.

**31. Correlate metrics, logs, traces.** Shared `service.name`, `trace_id` injected into logs, exemplars on histograms link metric to trace, Grafana cross-data-source linking (Tempo ↔ Loki ↔ Prometheus).

**32. Deploy Collector in K8s.** DaemonSet (per-node agent) for local receive + Deployment (gateway) for central processing. Helm chart `open-telemetry/opentelemetry-collector`.

**33. Secure telemetry.** TLS on OTLP endpoints, mTLS between agent and gateway, auth headers (API keys), redact PII via `attributes`/`transform` processors, network policies.

**34. Avoid vendor lock-in.** OTel SDK in app + OTel Collector with multiple exporters → switch backends by changing exporter config, no code changes.

**35. Pipeline for high traffic.** Two-tier: agent (DaemonSet) does local batch + light processing → gateway (Deployment, autoscaled) does enrichment + sampling → backend. Use memory_limiter, batch processors, persistent queue for backpressure.

---

## SECTION 10 — APM / NEW RELIC

### Easy

**1. APM.** Application Performance Monitoring — code-level visibility into request flow, transactions, latency, errors, slow queries, dependencies.

**2. Why used.** Quickly find which line/method/query slows the app; correlate traces and infra; reduce MTTR; baseline performance over releases.

**3. New Relic.** SaaS observability platform — APM, infra, logs, browser/mobile RUM, synthetics, alerts. Agent-based instrumentation, NRQL query language.

**4. APM definition.** Continuous, deep, code-aware monitoring of app performance — transactions, errors, throughput, dependency latency.

**5. Transaction tracing.** Captures a single request's path through code with timings per method/call (DB, external HTTP).

**6. Error tracking.** Automatic capture of exceptions/errors with stack trace, occurrence count, attributes, user impact.

**7. Response time.** Time from request received to response sent; usually reported as p50/p95/p99.

**8. Throughput.** Requests per minute/second processed by the service.

**9. Apdex.** Application Performance Index — score 0–1 based on satisfied/tolerated/frustrated thresholds (e.g., satisfied < 500ms, tolerated < 2s, frustrated > 2s).

**10. Service map.** Visual graph of services and their dependencies (DB, queues, downstream services), often with health overlay.

### Medium

**11. How NR monitors apps.** Language-specific agent (Java, .NET, Node, Python, Go) auto-instruments common frameworks, captures metrics/traces/errors, ships to NR over HTTPS.

**12. NR agent.** Library installed in the app (e.g., Java javaagent jar) that hooks into runtime to capture telemetry without code changes.

**13. Install agent.** Java: `-javaagent:newrelic.jar` + `newrelic.yml` with license key. Node: `require('newrelic')` at top + env var `NEW_RELIC_LICENSE_KEY`. Python: `newrelic-admin run-program ...`.

**14. Metrics collected.** Throughput, response time, error rate, Apdex, transactions, DB call latency, external service calls, JVM/CLR/runtime metrics, custom metrics via API.

**15. Monitor errors.** Errors UI shows top errors by count, stack trace, attributes, occurrence timeline. Alert on error_rate > threshold.

**16. Create alerts.** Alerts & AI → Conditions → NRQL or static threshold (e.g., `error_rate > 5%`), evaluation window, signal lost handling, notification channel.

**17. Distributed tracing.** Cross-service trace via NR distributed-tracing headers or W3C; visualize end-to-end span tree across services.

**18. RCA help.** Service map shows the chain; transaction trace shows slow span; error UI shows exception; logs in context show app log lines for that trace_id.

**19. Infra monitoring.** New Relic infra agent on hosts/VMs/containers reports CPU, memory, disk, processes, integrations (MySQL, Nginx, Kafka).

**20. NR vs Prometheus.** NR is hosted SaaS APM with code-level traces. Prometheus is OSS metrics-only TSDB you self-host. Often used together (Prometheus → NR via remote write).

**21. NR vs Grafana.** NR is end-to-end SaaS observability incl. data store. Grafana is visualization only over many backends. Grafana doesn't store telemetry by itself.

**22. APM vs infra monitoring.** APM = application code visibility (transactions, traces). Infra = host/cluster resource monitoring. Both needed for complete picture.

### Difficult

**23. Troubleshoot high response time.** Sort transactions by slowest, open the trace, identify slow span (DB / external / app code). Check throughput correlation, recent deploys, GC time, downstream latency.

**24. Slow DB queries.** APM Database tab shows slow query log with explain plan and call count. Sort by total time. Check indexes, locks, connection pool, query plan changes.

**25. Trace failed transaction across microservices.** Use Distributed Tracing UI; pick the failed trace; expand span tree; find the first errored span and read its attributes/logs.

**26. Reduce APM cost.** Sampling, drop high-volume low-value transactions, set retention, control custom event ingest, drop attribute-heavy logs, exclude health-check endpoints.

**27. Alert policies.** Group conditions by service/team, layered severity (warning + critical thresholds), suppression during deploys, baseline (anomaly) instead of static where possible, notification routing per team.

**28. Integrate with K8s.** New Relic Kubernetes integration (Helm chart) deploys infra agent + Pixie + Prometheus scraper + log collector + Kube events. Auto-discovers workloads.

**29. NR + OpenTelemetry.** Configure OTel SDK to export OTLP to NR endpoint with license key. NR ingests OTLP-native, no NR-specific agent needed.

**30. Customer experience monitoring.** Browser agent (RUM) — page load time, JS errors, AJAX latency, geographic split. Synthetics — scripted journeys from global locations.

**31. NR vs Datadog vs Dynatrace vs AppDynamics.** All SaaS APM; key differentiators: NR — broad, NRQL flexible. Datadog — strong infra+APM unification. Dynatrace — AI-driven RCA (Davis), one-agent. AppDynamics — enterprise, business transaction focus. Pricing models differ (host vs ingest vs user).

**32. Avoid false alerts.** Use baseline/anomaly conditions, multi-window thresholds, group by service, require sustained breach, exclude maintenance windows, monthly alert review.

---

## SECTION 11 — AWS

### Easy

**1. AWS.** Amazon Web Services — public cloud platform offering compute, storage, networking, databases, AI/ML, etc., as on-demand services.

**2. EC2.** Elastic Compute Cloud — virtual machines on demand. Pick AMI, instance type, network, storage; SSH/RDP in.

**3. S3.** Simple Storage Service — object storage; buckets and keys; 11 nines durability; versioning, lifecycle, encryption, replication.

**4. IAM.** Identity & Access Management — users, groups, roles, policies controlling access to AWS resources.

**5. VPC.** Virtual Private Cloud — isolated virtual network with subnets, route tables, IGW/NAT, security groups, NACLs.

**6. CloudWatch.** AWS native monitoring — metrics, logs, alarms, dashboards, events.

**7. CloudWatch Logs.** Centralized log service — log groups → log streams → log events. Filter, query (Logs Insights), set retention.

**8. CloudWatch Metrics.** Time-series metrics from AWS services and custom apps; alarms can trigger on metric breaches.

**9. CloudWatch Alarm.** Threshold-based alert on a metric → triggers SNS / Auto Scaling / Lambda action.

**10. EKS.** Elastic Kubernetes Service — managed Kubernetes control plane; you manage worker nodes (or use Fargate).

**11. ECS.** Elastic Container Service — AWS-native container orchestration; runs on EC2 or Fargate.

**12. Lambda.** Serverless functions — event-driven code with no server management; pay per request and duration.

**13. RDS.** Relational Database Service — managed MySQL, PostgreSQL, MariaDB, Oracle, SQL Server, Aurora.

**14. Auto Scaling.** Adjusts EC2/ECS/EKS capacity based on metrics or schedule.

**15. Load Balancer.** ELB family — ALB (L7 HTTP), NLB (L4 TCP/UDP), GWLB (L3 gateway), CLB (legacy).

**16. Security Group.** Stateful firewall at ENI level; default-deny inbound, default-allow outbound; rules by port/protocol/source.

**17. Route 53.** DNS service — domain registration, hosted zones, health checks, traffic routing policies (weighted, latency, failover, geo).

### Medium

**18. Monitor EC2.** CloudWatch metrics (CPU, network, disk by default; memory & disk via CW agent); Logs via CW agent or syslog/journald shipping to CloudWatch Logs; alarms on CPU/StatusCheckFailed.

**19. App logs in CW.** Install CloudWatch agent on EC2/ECS/EKS → configures log groups → ships to CW Logs. Query via Logs Insights.

**20. CW alarms.** CloudWatch → Alarms → Create → metric + threshold + period + evaluation periods + SNS topic action.

**21. CloudWatch vs CloudTrail.** CW = operational metrics/logs/alarms. CloudTrail = audit log of API calls (who did what, when, from where).

**22. SG vs NACL.** SG: ENI-level, stateful, allow-only. NACL: subnet-level, stateless (must allow return traffic), allow+deny rules. SG primary control; NACL coarse extra layer.

**23. Public vs private subnet.** Public: route table has 0.0.0.0/0 → IGW; instances can have public IPs. Private: route 0.0.0.0/0 → NAT gateway; outbound-only internet, no inbound from internet.

**24. IAM role.** Identity assumable by AWS services or trusted principals; carries permissions via attached policies. Avoids long-lived keys (used by EC2, Lambda, EKS, cross-account).

**25. Jenkins → AWS.** Use IAM role on Jenkins EC2 (instance profile) or IRSA on EKS; pipelines call AWS CLI/SDK which auto-uses the role. Avoid storing keys in Jenkins.

**26. Terraform → AWS.** Configure AWS provider with assumed role (preferred), instance profile, or env-var creds. Use S3 + DynamoDB backend for state and lock.

**27. AWS CLI.** `aws` command-line tool for invoking AWS APIs from terminal/scripts. Configured via `aws configure` (creds + region).

**28. IAM policy.** JSON document with `Effect`, `Action`, `Resource`, `Condition`. Attached to user/group/role.

**29. IAM user vs role.** User: long-lived identity with credentials. Role: temporary identity assumed via STS, returns time-bounded creds. Always prefer roles for workloads and federation.

**30. Monitor EKS.** Container Insights, Prometheus + Grafana via kube-prometheus-stack, CloudWatch Logs via Fluent Bit, Control Plane logs in CloudWatch (enable in EKS console).

**31. Logs from EKS.** Fluent Bit DaemonSet → CloudWatch Logs (or ES/OpenSearch). EKS control plane logs (audit/api/scheduler/controllerManager) → CloudWatch.

**32. Monitor RDS.** CW metrics (CPUUtilization, DatabaseConnections, FreeableMemory, ReadIOPS), Performance Insights for query-level, Enhanced Monitoring for OS metrics, slow query log to CW.

**33. Monitor ALB.** CW metrics: RequestCount, TargetResponseTime, HTTPCode_Target_5XX_Count, UnHealthyHostCount. Access logs to S3 → Athena for forensics.

**34. EC2 high CPU.** Check CW metric trend, SSH and run `top`/`htop`, identify process, look at recent deploys, runaway loops, memory pressure causing swap. Scale up/out as mitigation.

**35. App downtime in AWS.** Check ALB target health, ASG instance health, CW alarms, recent deploys, RDS connectivity, IAM/SG changes. Use service dashboard for each component.

### Difficult

**36. Design AWS infra monitoring.** CloudWatch as base layer, Container Insights for ECS/EKS, Prometheus+Grafana for K8s, central account for cross-account observability (CW cross-account sharing), VPC Flow Logs to S3+Athena, GuardDuty for security, Config for compliance.

**37. Centralized logging in AWS.** CW Logs subscription filter → Kinesis Firehose → S3 / OpenSearch. Or Fluent Bit → OpenSearch. Use Logs Insights or Kibana for search.

**38. Multi-account AWS.** AWS Organizations + central monitoring account; CW cross-account observability; Control Tower for governance; SCPs for guardrails; CloudTrail Organization Trail to central S3.

**39. Secure AWS creds in Jenkins.** No static keys — use instance profile or IRSA; Jenkins `aws-credentials-plugin` referencing IAM role; or AWS Secrets Manager / Vault retrieved at runtime.

**40. Least privilege IAM.** Start with empty policies, grant minimum actions per resource, use IAM Access Analyzer to refine, use permission boundaries on roles, review CloudTrail unused permissions.

**41. EKS worker node issues.** `kubectl get nodes`; SSM into node; check kubelet, containerd, CNI plugin, disk pressure, ASG launch template, IAM role (node group), security group, subnet routing.

**42. ALB 502/503.** 502 = bad gateway (target returned bad response, idle timeout, mismatched HTTP). 503 = no healthy targets. Check target health, TG threshold, SG rules, app readiness probe, idle_timeout.

**43. High latency in AWS.** Trace via X-Ray or APM; check ALB TargetResponseTime, RDS slow queries, NAT gateway saturation, ENI bandwidth, region/AZ affinity, cross-AZ data transfer.

**44. CW + Prometheus/Grafana.** Use CloudWatch exporter (YACE) → Prometheus; or Grafana CloudWatch data source directly; or Prometheus remote write to AMP (managed Prometheus).

**45. CW Logs → Elasticsearch.** Subscription filter → Kinesis Firehose → OpenSearch; or Lambda subscriber that PUTs to ES; or Fluent Bit reading directly.

**46. Automate AWS with Terraform.** Modules per environment, S3+DynamoDB backend, CI/CD pipeline running plan on PR + apply on merge, Sentinel/OPA policies, drift detection cron.

**47. Manage secrets.** AWS Secrets Manager (rotation, KMS) or SSM Parameter Store (cheaper, no native rotation). Inject into apps via IAM-authenticated SDK. Don't store in env vars or AMIs.

**48. HA architecture.** Multi-AZ everything (ALB, ASG min across 3 AZs, RDS Multi-AZ, ElastiCache replication group), Route 53 health-check failover, S3 cross-region replication, regional active-active or active-passive.

**49. Cost monitoring.** Cost Explorer, Cost Anomaly Detection, Budgets with alerts, tagging strategy + Cost Allocation Tags, Trusted Advisor cost checks, Compute Optimizer, savings plans/RIs.

**50. Lambda failures.** CW Logs (each invocation), CW metrics (Errors, Throttles, Duration), X-Ray traces, DLQ for async failures, retry configuration, concurrent execution limits.

---

## SECTION 12 — TERRAFORM

### Easy

**1. Terraform.** HashiCorp's open-source IaC tool using HCL to declaratively provision and manage cloud/SaaS resources via providers.

**2. Why used.** Idempotent infra, version-controlled, multi-cloud, plan-before-apply preview, modular reuse, drift detection.

**3. IaC.** Infrastructure as Code — defining infra (servers, networks, IAM) in version-controlled files instead of manual console clicks.

**4. Provider.** Plugin that knows how to talk to a specific platform's API (aws, azurerm, google, kubernetes, github, helm).

**5. Resource.** A managed object (`aws_instance`, `aws_s3_bucket`); has a type, name, and arguments.

**6. Variable.** Parameter for a module; declared in `variable` block with type/default/description.

**7. Output.** Exported value from a module; declared in `output` block; visible after apply, queryable via `terraform output`.

**8. terraform init.** Downloads providers, initializes backend, sets up modules. Run once per workspace/checkout.

**9. terraform plan.** Computes diff between desired state (HCL) and actual state; shows what will be created/changed/destroyed. No changes made.

**10. terraform apply.** Executes the plan against the cloud API; writes new state.

**11. terraform destroy.** Tears down all resources in the state. Use with care.

**12. Terraform state.** JSON file mapping HCL resources to real cloud IDs and recording last-known attributes. Source of truth for diffs.

**13. Module.** Reusable bundle of HCL — directory with resources/variables/outputs. Called via `module` block.

**14. Backend.** Where state is stored — local file (default) or remote (S3, Terraform Cloud, GCS, AzureRM, Consul).

### Medium

**15. Terraform workflow.** Write HCL → `init` → `plan` (review) → `apply` → state updated. In CI/CD: PR triggers plan; merge triggers apply.

**16. State file purpose.** Maps HCL resource addresses to real cloud resource IDs; records attribute values to compute diffs; supports dependency graph.

**17. Remote backend.** Stores state in shared remote location (S3) so team members and CI use the same state. Enables locking.

**18. Why not local in teams.** Conflicts, no locking, lost laptop = lost state, no audit trail.

**19. State in AWS.**
```hcl
terraform {
  backend "s3" {
    bucket = "tfstate-prod"
    key    = "network/terraform.tfstate"
    region = "us-east-1"
    dynamodb_table = "tfstate-lock"
    encrypt = true
  }
}
```

**20. DynamoDB locking.** A DynamoDB table with `LockID` partition key holds a lock during apply, preventing concurrent runs from corrupting state.

**21. Drift.** Real infra differs from state file (someone changed it manually). `terraform plan` detects drift and proposes reconciliation.

**22. Taint.** Marks a resource for forced recreation on next apply. Modern syntax: `terraform apply -replace=aws_instance.web`.

**23. terraform import.** Brings existing manually-created resource under Terraform management: `terraform import aws_s3_bucket.b my-bucket`. Then write matching HCL.

**24. count vs for_each.** `count = N` creates N indexed copies (`[0]`, `[1]`). `for_each = toset(...)` or map creates keyed copies — preferred when items have stable identity (changing a list reorders indices and forces recreation).

**25. variable vs local.** Variable: external input (override per env). Local: internal computed value, not user-overridable.

**26. data source vs resource.** Resource: managed object (created/modified by TF). Data source: read-only lookup of an existing object (`data.aws_ami.latest`).

**27. Workspaces.** Named state branches within one backend (`default`, `dev`, `prod`). Often misused as env separation — better to use separate backends per env.

**28. Multiple environments.** Recommended: separate root modules per env (`environments/dev`, `environments/prod`) sharing modules, separate state. Avoid workspaces for env split.

**29. Pass secrets.** Don't hardcode. Use TF_VAR env vars at runtime, pull from Vault / AWS Secrets Manager via data source, mark variable `sensitive = true` to suppress in logs.

**30. Terraform with Jenkins.** Pipeline: `init → fmt → validate → plan` (post plan output as PR comment); on merge: manual approval → `apply`. Store state in S3, lock via DynamoDB. Use IAM role on agent.

**31. Terraform with AWS.** AWS provider with `assume_role` block to a deploy role; remote state in S3+DynamoDB; tag everything; module-per-domain (network, eks, rds, iam).

**32. Provider version locking.** `required_providers { aws = { version = "~> 5.0" } }`. `.terraform.lock.hcl` records exact versions and hashes for reproducibility.

**33. .terraform.lock.hcl.** Lock file pinning provider versions and checksums. Commit to Git for reproducible builds across machines.

### Difficult

**34. State corruption.** Restore from S3 versioning / backup. `terraform state pull > backup.tfstate` regularly. Use `terraform state rm/mv` carefully. Avoid editing state JSON manually.

**35. State file deleted.** Without state, TF doesn't know what it manages → next apply tries to recreate everything. Recover from S3 versioning or rebuild via `terraform import` for each resource.

**36. Recover failed apply.** Read error, fix HCL or external dependency, re-run `apply`. State partially updated will reconcile on next plan. For a stuck lock, `terraform force-unlock <id>` after confirming nobody else is running.

**37. Detect & fix drift.** Scheduled `terraform plan` in CI (drift detection); compare to "no changes". Fix by re-applying, or by importing the manual change into HCL.

**38. Modules at enterprise scale.** Versioned modules in private registry / Git tags; semantic versioning; CHANGELOG; CODEOWNERS; central platform team owns base modules.

**39. Structure for dev/test/prod.** `modules/` (shared), `environments/{dev,test,prod}/main.tf` (root, calls modules with env-specific vars), separate state per env, separate IAM roles per env.

**40. Secure state.** S3 bucket encryption (KMS), bucket policy denying public, versioning on, MFA-delete optional, DynamoDB table encrypted, access only via deploy role.

**41. Secrets without exposing in state.** Mark `sensitive = true`. Use ephemeral `data` lookups (Vault, Secrets Manager) at apply. Be aware: state still contains the value — encrypt state and restrict access.

**42. Review TF in CI/CD.** Auto-run `fmt`, `validate`, `tflint`, `tfsec`/`checkov`; post `plan` to PR; require approval; apply on merge. Block destroys without ticket.

**43. Prevent accidental destroy.**
```hcl
lifecycle { prevent_destroy = true }
```
Plus: separate destroy pipeline, manual approvals, IAM deny on `Delete*` for prod.

**44. lifecycle block.** Resource-level meta-args: `prevent_destroy`, `create_before_destroy`, `ignore_changes`, `replace_triggered_by`.

**45. create_before_destroy.** Creates the new resource before destroying the old — essential for zero-downtime replacement (LB target groups, IAM with attached policies).

**46. ignore_changes.** Tells TF not to revert specific attributes if changed out-of-band (e.g., ASG `desired_capacity` managed by autoscaling).

**47. Dependencies.** Implicit via interpolation (`aws_subnet.a.id` referenced in another resource). Explicit via `depends_on = [aws_iam_role_policy.x]` for hidden runtime deps.

**48. Implicit vs explicit.** Implicit: TF infers from references. Explicit: developer-declared `depends_on`. Use explicit only when no reference exists.

**49. Provider creds securely.** Assume roles with OIDC (GitHub Actions, GitLab), instance profile on Jenkins agent, never long-lived keys. Use Sentinel/OPA to enforce.

**50. Approval workflow for apply.** PR → plan posted → reviewer approves → merge to main → apply pipeline with manual `Proceed` gate before destructive change. Use Terraform Cloud / Atlantis / Spacelift for managed approval flow.

---

## SECTION 13 — JENKINS

### Easy

**1. Jenkins.** Open-source automation server for CI/CD; runs jobs/pipelines defined in code, with a vast plugin ecosystem.

**2. Why used.** Automate build, test, deploy; integrate with SCM/cloud/K8s; widely adopted; flexible pipeline DSL.

**3. CI/CD.** Continuous Integration (auto build/test on every commit) + Continuous Delivery/Deployment (auto-promote artifacts through environments).

**4. Job.** A single configurable task (legacy: Freestyle); modern equivalent is a Pipeline.

**5. Pipeline.** A workflow as code in `Jenkinsfile`, supporting stages, parallelism, agents, conditions, post actions.

**6. Jenkinsfile.** Groovy file in repo defining the pipeline; declarative or scripted syntax.

**7. Stage.** Logical phase of a pipeline (Checkout, Build, Test, Deploy) — visualized in Blue Ocean / Stage View.

**8. Step.** A single command/task within a stage (`sh 'mvn package'`, `git`, `docker.build`).

**9. Build.** A single execution of a job/pipeline; produces logs, artifacts, test results.

**10. Artifact.** Output of a build (jar, war, docker image, terraform plan) preserved by the pipeline.

**11. Plugin.** Extension adding capabilities (Git, Pipeline, Kubernetes, AWS Steps, Slack, Docker).

**12. Controller.** The main Jenkins server (web UI, scheduler, config, plugins). Formerly *master*.

**13. Agent.** Worker node executing pipeline steps. Formerly *slave*.

**14. Master/slave terminology.** Renamed to *controller/agent* to remove offensive terminology — same concept.

**15. Build trigger.** What initiates a build: SCM webhook, poll SCM, periodic (cron), upstream job, manual, API.

### Medium

**16. Jenkins pipeline (declarative).**
```groovy
pipeline {
  agent { kubernetes { yaml '...' } }
  stages {
    stage('Checkout') { steps { checkout scm } }
    stage('Build')    { steps { sh 'mvn -B package' } }
    stage('Test')     { steps { sh 'mvn test' } }
    stage('Deploy')   { when { branch 'main' } steps { sh './deploy.sh' } }
  }
  post { failure { slackSend channel:'#alerts', message:"Build failed" } }
}
```

**17. Declarative vs scripted.** Declarative: structured, opinionated `pipeline { ... }`, easier to read. Scripted: pure Groovy, more flexible but harder to maintain. Prefer declarative.

**18. Controller-agent architecture.** Controller manages config/UI/scheduler; dispatches builds to agents over JNLP/SSH/Kubernetes; agents execute and stream logs/artifacts back.

**19. Communication with agents.** Inbound (JNLP — agent connects to controller) or outbound (SSH from controller). Modern: Kubernetes plugin spawns ephemeral pod agents per build.

**20. Configure credentials.** Manage Jenkins → Credentials → Add — types: Username/password, Secret text, Secret file, SSH private key, Certificate, AWS, Kubernetes config. Reference in pipeline via `withCredentials`.

**21. Integrate with GitHub.** GitHub plugin + webhook to `/github-webhook/`; Multibranch Pipeline auto-discovers branches/PRs; PAT stored as credential for API access.

**22. Integrate with AWS.** IAM role on Jenkins host (instance profile) or IRSA on EKS; AWS Steps plugin or just `sh "aws ..."`. Avoid storing access keys.

**23. Run Terraform.**
```groovy
stage('TF Plan') {
  steps {
    sh 'terraform init'
    sh 'terraform plan -out=tfplan'
  }
}
stage('TF Apply') {
  when { branch 'main' }
  input message: 'Apply?'
  steps { sh 'terraform apply tfplan' }
}
```

**24. Deploy to K8s.** `kubectl apply` or Helm: `helm upgrade --install app charts/app -f values.yaml`. Auth via in-cluster ServiceAccount or kubeconfig credential.

**25. Multibranch pipeline.** Job type that scans a repo for branches/PRs containing a `Jenkinsfile` and creates/destroys sub-jobs automatically.

**26. Webhook.** HTTP callback from SCM (GitHub, GitLab) to Jenkins on push/PR; triggers a build instantly. Preferred over polling.

**27. Polling SCM.** Jenkins periodically asks SCM if there are changes; less efficient than webhooks.

**28. Workspace.** Per-job directory on the agent where checkout and build happen (`/var/lib/jenkins/workspace/<job>`); ephemeral with K8s agents.

**29. Archive artifacts.** `archiveArtifacts artifacts: 'target/*.jar', fingerprint: true` — preserves files in build record for download.

**30. Handle secrets.** Always via Credentials store; reference with `credentials('my-secret')` env binding or `withCredentials` block. Mask in logs. Never echo.

**31. Parameterize pipelines.** Declarative `parameters { string(name:'ENV', defaultValue:'dev') choice(...) booleanParam(...) }`. Trigger with parameters via UI/API.

**32. Notifications.** Slack/Teams/email plugins in `post` block — `success`, `failure`, `unstable`, `always`. Include build URL + commit + author.

**33. Post-build actions.** Things that run after main pipeline: archive artifacts, publish JUnit/HTML reports, notify, clean workspace, deploy on success.

### Difficult

**34. CI/CD for microservices.** Multibranch per service, shared library for common steps, parallel test stages, image build → ECR/GHCR, Helm deploy with values per env, gated promotion (dev → staging → prod with manual approval), rollback step.

**35. Build failures.** Read console output → identify failing stage/step → reproduce locally → check env diffs (tool versions, env vars, network), recent dependency updates, flaky tests, agent disk space.

**36. Agent offline.** Check agent logs (`/var/log/jenkins-agent.log`), network to controller, JNLP port (50000) firewall, JVM crash, disk full, agent process running, secret mismatch.

**37. Secure Jenkins.** Enable Matrix Authorization / Role-Based Strategy plugin, integrate with LDAP/SAML, restrict anonymous, enable CSRF protection, disable Script Console for non-admin, run agents with least privilege, regular plugin updates.

**38. Manage credentials safely.** Folder-scoped credentials, secret files, no plaintext in Jenkinsfile, mask in logs, rotate, audit usage. Integrate with Vault/AWS Secrets Manager via plugin.

**39. K8s agents.** Kubernetes plugin spawns pod-per-build; pod template specifies containers (jnlp, maven, kaniko, kubectl). Ephemeral, scalable, isolated.

**40. Scale Jenkins.** Add agents (static or K8s dynamic); offload artifacts to S3/Artifactory; increase controller heap; split into per-team controllers (CloudBees CJOC); use cloud agents.

**41. Shared libraries.** Groovy code in a Git repo (`vars/`, `src/`) loaded by `@Library('mylib')`. Centralizes pipeline patterns; versioned by Git tag/branch.

**42. Approval before prod.** `input message: 'Deploy to prod?', submitter: 'sre-team'` step pauses pipeline; only allowed users can approve. Combine with restricted credentials.

**43. Rollback.** Keep previous Helm release: `helm rollback app <revision>`. Or tag-based: deploy previous image tag. Pipeline stage triggered manually or auto on health check failure.

**44. Parallel stages.**
```groovy
stage('Tests') {
  parallel {
    stage('Unit')        { steps { sh 'mvn test' } }
    stage('Integration') { steps { sh 'mvn verify' } }
    stage('Lint')        { steps { sh 'npm run lint' } }
  }
}
```

**45. Pipeline timeout.**
```groovy
options { timeout(time: 30, unit: 'MINUTES') }
```
Or per-stage `timeout(...) { ... }`.

**46. Avoid hardcoded creds.** Always Credentials store; pre-commit hooks scanning for keys; gitleaks in CI; rotate on suspicion of leak; use temporary credentials (STS).

**47. Jenkins → cloud platforms.** Via SDK/CLI authenticated by IAM role / service principal / GCP service account. For ephemeral creds: OIDC federation to AWS/GCP/Azure (no static keys).

**48. Jenkins auth with AWS.** Instance profile (preferred), IRSA (on EKS), Jenkins AWS Credentials plugin storing access key (last resort), or AssumeRoleWithWebIdentity via OIDC.

**49. Monitor Jenkins.** Prometheus plugin exposes `/prometheus` metrics — queue depth, executor utilization, build duration, failures. Build status via Slack. Audit log plugin.

**50. Backup & restore.** Backup `JENKINS_HOME` (config, jobs, plugins, secrets, credentials.xml) to S3 daily. Use ThinBackup plugin or filesystem snapshot. Restore by extracting to new instance and starting Jenkins.

---

## SECTION 14 — SHELL / BASH SCRIPTING

### Easy

**1. Shell scripting.** Writing programs interpreted by a shell (bash, sh, zsh) to automate command-line tasks.

**2. Bash.** Bourne Again SHell — default shell on most Linux distros; superset of POSIX `sh`.

**3. Create a script.** Write commands to `script.sh`, first line `#!/usr/bin/env bash` (shebang), make executable.

**4. Execute.** `./script.sh` (executable + on PATH or with relative path) or `bash script.sh`.

**5. chmod +x.** Adds execute permission so the file can be run directly.

**6. echo.** Prints arguments to stdout. Use `printf` for portable formatting.

**7. Variable.** `name="value"`; access with `$name` or `${name}`. No spaces around `=`.

**8. $?.** Exit code of the last command (0 = success, non-zero = error).

**9. $0, $1, $2.** Script name and positional arguments. `$#` = arg count, `$@` = all args.

**10. if-else.**
```bash
if [[ -f "$file" ]]; then echo "exists"; else echo "missing"; fi
```

**11. Loop.**
```bash
for f in *.log; do echo "$f"; done
while read -r line; do echo "$line"; done < file
```

**12. cron.** Time-based job scheduler. `crontab -e` to edit, format `m h dom mon dow command`. Example: `0 2 * * * /usr/local/bin/backup.sh`.

### Medium

**13. Disk usage script.**
```bash
#!/usr/bin/env bash
THRESHOLD=85
df -P | awk 'NR>1 {gsub("%","",$5); if ($5+0 > '"$THRESHOLD"') print $6,$5"%"}'
```

**14. CPU usage script.**
```bash
top -bn1 | awk '/Cpu\(s\)/ {print 100-$8"%"}'
```

**15. Process check.**
```bash
pgrep -x nginx >/dev/null && echo "running" || echo "stopped"
```

**16. Restart if down.**
```bash
if ! systemctl is-active --quiet nginx; then systemctl restart nginx; fi
```

**17. Find large logs.** `find /var/log -type f -size +100M -exec ls -lh {} \;`

**18. Archive old logs.** `find /var/log -name '*.log' -mtime +30 -exec gzip {} \;`

**19. grep.** Pattern search: `grep -i "error" app.log`, `grep -E "5[0-9]{2}"`, `-v` invert, `-r` recursive, `-c` count.

**20. awk.** Field-oriented processor: `awk '$5 > 100 {print $1,$5}' file` — splits each line into `$1..$NF`, runs actions.

**21. sed.** Stream editor: `sed -i 's/old/new/g' file` substitution; `sed -n '10,20p'` print line range; `sed -i '/pattern/d'` delete matching lines.

**22. Read file line by line.**
```bash
while IFS= read -r line; do
  echo ">> $line"
done < input.txt
```

**23. Pass arguments.** `script.sh arg1 arg2` — accessed as `$1`, `$2`. Use `getopts` for flags: `while getopts ":h:p:" opt; do case $opt in h) HOST=$OPTARG;; esac; done`.

**24. Schedule via cron.** Edit `crontab -e`, add `*/5 * * * * /opt/scripts/check.sh >> /var/log/check.log 2>&1`.

**25. Error handling.**
```bash
set -euo pipefail
trap 'echo "Error on line $LINENO"; cleanup' ERR
```

**26. Redirect to file.** `cmd > out.txt` (overwrite stdout), `cmd >> out.txt` (append), `cmd 2> err.txt` (stderr).

**27. > vs >>.** `>` overwrites; `>>` appends.

**28. 2> vs 2>&1.** `2>` redirects stderr to file; `2>&1` redirects stderr to wherever stdout currently points. `cmd > out.log 2>&1` puts both into out.log.

### Difficult

**29. Production-ready scripts.** `#!/usr/bin/env bash`, `set -euo pipefail`, `IFS=$'\n\t'`, structured logging, exit codes, idempotent operations, locking (`flock`), CLI flags, dry-run mode, traps for cleanup.

**30. Debug scripts.** `bash -x script.sh`, add `set -x` in code, `PS4='+ ${BASH_SOURCE}:${LINENO} '` for richer trace, `shellcheck` for static analysis.

**31. set -e.** Exit on any command failure (non-zero return).

**32. set -x.** Print each command before executing — debugging.

**33. set -u.** Treat unset variables as errors.

**34. Signals.**
```bash
cleanup() { rm -f /tmp/lock; }
trap cleanup EXIT INT TERM
```
Catches Ctrl-C, kill, normal exit.

**35. Real-time log monitoring.** `tail -F /var/log/app.log | grep --line-buffered -i "error" | while read -r line; do alert "$line"; done`.

**36. Parse logs with awk/sed.** Awk count by status:
```bash
awk '{c[$9]++} END {for(k in c) print k,c[k]}' access.log
```

**37. Multi-server health check.** Loop over hosts, ssh and run check, aggregate results.
```bash
for h in $(cat hosts); do ssh -o ConnectTimeout=5 "$h" 'uptime' || echo "$h DOWN"; done
```

**38. Automate deployment.** Pull artifact → stop service → backup current → install new → start service → health-check → rollback on failure (trap ERR).

**39. Securely handle passwords.** Read with `read -s`, never echo, never `set -x` near secrets, use OS keyring / Secret Manager API; mask in logs; use temporary files with `mktemp` + `chmod 600` + trap-cleanup.

**40. Idempotent scripts.** Check before action: `[[ -d /opt/app ]] || mkdir /opt/app`; `id -u user >/dev/null || useradd user`; ensure repeated runs produce same final state.

---

## SECTION 15 — PYTHON SCRIPTING

### Easy

**1. Python.** High-level, interpreted, dynamically typed language with extensive standard library; common in DevOps/automation/data.

**2. Why in DevOps.** Rich SDKs (boto3, kubernetes, requests), readable, cross-platform, fast to prototype, large ecosystem (Ansible is Python).

**3. List, tuple, dict.** List: ordered, mutable `[1,2]`. Tuple: ordered, immutable `(1,2)`. Dict: key-value, hash-based `{"k":1}`.

**4. Function.** `def name(args): ...` — reusable block; supports defaults, *args, **kwargs, type hints.

**5. Exception handling.** `try: ... except SomeError as e: ... finally: ...`.

**6. Module.** A `.py` file (or package directory). Imported via `import name`.

**7. pip.** Python package installer; `pip install requests`. Use `pip freeze > requirements.txt`.

**8. Read a file.**
```python
with open("file.txt") as f:
    for line in f:
        process(line.rstrip())
```

**9. Write to a file.**
```python
with open("out.txt", "w") as f:
    f.write("hello\n")
```

**10. Parse JSON.**
```python
import json
data = json.loads(text)        # str → dict
text = json.dumps(data, indent=2)
```

**11. Virtual environment.** Isolated Python install per project — `python -m venv .venv && source .venv/bin/activate`. Avoids dependency conflicts.

### Medium

**12. Read log file.**
```python
with open("/var/log/app.log") as f:
    for line in f:
        if "ERROR" in line:
            print(line.rstrip())
```

**13. Find errors in logs.**
```python
import re
pat = re.compile(r"\bERROR\b|\bException\b")
with open(path) as f:
    errors = [l for l in f if pat.search(l)]
```

**14. Call an API.**
```python
import requests
r = requests.get("https://api.example.com/v1/health", timeout=5)
r.raise_for_status()
print(r.json())
```

**15. requests library.** HTTP client — `get/post/put/delete`, JSON helpers, timeouts, sessions for connection pooling, auth, retries via `urllib3`.

**16. API response codes.**
```python
if r.status_code == 200: ...
elif 500 <= r.status_code < 600: retry()
```
Or `r.raise_for_status()` to raise on 4xx/5xx.

**17. Parse JSON response.** `r.json()` returns dict/list. Validate with `jsonschema` or pydantic.

**18. Send email.**
```python
import smtplib
from email.message import EmailMessage
m = EmailMessage(); m["From"]="x"; m["To"]="y"; m["Subject"]="hi"; m.set_content("body")
with smtplib.SMTP("smtp.example.com",587) as s:
    s.starttls(); s.login(u,p); s.send_message(m)
```

**19. Interact with AWS.** Use `boto3`:
```python
import boto3
ec2 = boto3.client("ec2")
print(ec2.describe_instances())
```

**20. boto3.** Official AWS SDK for Python — clients (`boto3.client`) and resources (`boto3.resource`); auto-discovers credentials from env / role / profile.

**21. EC2 status.**
```python
ids = [i["InstanceId"] for r in ec2.describe_instances()["Reservations"] for i in r["Instances"]]
print(ec2.describe_instance_status(InstanceIds=ids))
```

**22. Upload to S3.**
```python
boto3.client("s3").upload_file("local.txt", "my-bucket", "key.txt")
```

**23. Health-check script.**
```python
import requests, sys
try:
    r = requests.get(sys.argv[1], timeout=5)
    sys.exit(0 if r.ok else 1)
except Exception:
    sys.exit(2)
```

**24. Exception handling in automation.** Catch specific exceptions (`requests.RequestException`, `botocore.exceptions.ClientError`); log + retry / alert; never bare `except:`.

### Difficult

**25. Monitoring automation.** Modular script — config (YAML), checks per service, parallel via `concurrent.futures`, output to Prometheus pushgateway / CW metric / Slack alert. Schedule via cron / systemd timer.

**26. Incident automation.** Listen to Alertmanager webhook (Flask endpoint) → enrich (kube events, recent deploys) → post to Slack with runbook. Auto-restart pod / rotate node based on alert tags.

**27. Reusable modules.** Package shared utilities (`pip install -e .`); type hints; unit tests; semantic versioning; private PyPI / Artifactory.

**28. API retries.**
```python
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
s = requests.Session()
s.mount("https://", HTTPAdapter(max_retries=Retry(total=5, backoff_factor=0.5, status_forcelist=[502,503,504])))
```

**29. Timeouts.** Always pass `timeout=(connect, read)` to requests; default is no timeout (hangs forever). Combine with retries.

**30. Secure credentials.** Use AWS Secrets Manager / Vault / env vars; never hardcode. `keyring` for desktop, IAM roles for cloud, `os.environ` from CI secrets.

**31. Process large logs.** Stream line-by-line (don't `read()` whole file); use generators; `mmap` for random access; multiprocessing for parallel parsing; for compressed: `gzip.open`.

**32. K8s API.**
```python
from kubernetes import client, config
config.load_incluster_config()
v1 = client.CoreV1Api()
print(v1.list_namespaced_pod("default"))
```

**33. Integrate with Jenkins.** Pipeline `sh 'python check.py'`; Python script reads `BUILD_NUMBER` etc env vars; emits exit code; uploads results as artifact.

**34. Custom Prometheus metrics.**
```python
from prometheus_client import Counter, start_http_server
REQS = Counter("my_requests_total", "Total reqs", ["status"])
start_http_server(8000)
REQS.labels(status="200").inc()
```

---

## SECTION 16 — JAVASCRIPT BASICS

### Easy

**1. JavaScript.** Dynamic, single-threaded, event-loop-based language; runs in browsers and Node.js.

**2. Where used.** Front-end UI, Node.js back-end, build tools, dashboards (Grafana plugins), serverless functions, scripting.

**3. Node.js.** Server-side JS runtime built on V8; non-blocking I/O; npm ecosystem.

**4. npm.** Node Package Manager; install deps from `package.json`; `npm install`, `npm run`, `npm publish`.

**5. JSON.** JavaScript Object Notation — text format `{"k":1}`. Native to JS via `JSON.parse` / `JSON.stringify`.

**6. Function.** `function name(){}` or arrow `const f = () => {};`. First-class values.

**7. Variable.** Declared with `var` (function scope), `let` (block scope), `const` (block scope, no rebinding).

**8. var/let/const.** `var`: hoisted, function-scoped, can redeclare. `let`: block-scoped, no redeclare. `const`: block-scoped, immutable binding (object contents still mutable).

### Medium

**9. Call API.**
```js
const r = await fetch('https://api.example.com', { headers:{Authorization:'Bearer X'} });
const data = await r.json();
```

**10. async/await.** Syntactic sugar over Promises; `async` makes a function return a Promise; `await` pauses for resolution.

**11. Promise.** Object representing eventual completion of an async op. `.then()`, `.catch()`, `.finally()`. `Promise.all`, `Promise.race`.

**12. Event loop.** Single thread, callback queue + microtask queue; processes queued callbacks once stack is empty. Non-blocking I/O.

**13. Parse JSON.** `const obj = JSON.parse(text)`.

**14. Error handling.** `try { await op() } catch (e) { log(e) }`. Unhandled rejection events handled at process level.

**15. JS in dashboards.** Grafana custom plugins, internal SaaS dashboards, alert UIs, embedded widgets.

### Difficult

**16. Node.js health-check endpoint.**
```js
const http = require('http');
http.createServer((req,res) => {
  if (req.url === '/healthz') { res.writeHead(200); res.end('ok'); }
  else { res.writeHead(404); res.end(); }
}).listen(3000);
```

**17. Instrument Node.js.** Use OpenTelemetry Node SDK with auto-instrumentation:
```js
require('@opentelemetry/auto-instrumentations-node/register');
```
Or APM agent (`require('newrelic')` first).

**18. Collect logs.** Use `pino` or `winston` for structured JSON; pipe stdout to log collector (Fluent Bit) in containerized envs; include `trace_id` from OTel context.

**19. OTel in Node.js.** `npm i @opentelemetry/api @opentelemetry/sdk-node @opentelemetry/auto-instrumentations-node`. Configure OTLP exporter pointing to Collector.

**20. Memory leaks.** Heap snapshots via `--inspect` + Chrome DevTools; `clinic.js`, `heapdump`. Look at retained objects; common causes: closures, caches without eviction, listeners not removed.

---

## SECTION 17 — CI/CD & DEVOPS

### Easy

**1. DevOps.** Cultural + technical practice unifying development and operations to deliver software faster and more reliably via automation, monitoring, and shared ownership.

**2. CI.** Continuous Integration — every commit triggers automated build + test on shared branch.

**3. CD.** Continuous Delivery (auto-promote to staging, manual prod) or Continuous Deployment (auto to prod after tests pass).

**4. Delivery vs Deployment.** Delivery: artifact ready to deploy any time, manual prod release. Deployment: every passing build auto-deploys to prod.

**5. Pipeline.** Automated workflow of stages from code commit to deployment.

**6. Build.** Compile/package source into deployable artifact (jar, container image).

**7. Test automation.** Automated unit/integration/E2E tests run on every build to catch regressions.

**8. Deployment.** Pushing the artifact to a target environment.

**9. Rollback.** Reverting to a previous known-good version after a bad deploy.

**10. Version control.** Tools (Git) tracking code changes, branches, history.

**11. Git.** Distributed version control system; local commits, branches, merging, tags.

**12. GitHub/GitLab/Bitbucket.** SaaS Git hosting with PR/MR, CI, issues, wikis. GitHub also offers Actions; GitLab has built-in CI; Bitbucket pairs with Jira.

### Medium

**13. Typical CI/CD pipeline.** Checkout → Lint → Unit test → Build → Security scan → Push artifact → Deploy to dev → Integration test → Deploy to staging → E2E → Manual approval → Deploy to prod → Smoke test → Notify.

**14. Stages included.** Code quality (lint, format), security (SCA, SAST, container scan, IaC scan), build, test, package, deploy, post-deploy verification.

**15. Artifact management.** Store artifacts in a registry (Nexus, Artifactory, ECR, GHCR) with immutable tags + retention policies.

**16. Docker image build.** Dockerfile → `docker build -t app:tag .` → tag → push to registry. Use multi-stage to reduce size.

**17. Image scanning.** Scan for CVEs (Trivy, Grype, Snyk, Anchore, ECR scan); fail build on high/critical findings.

**18. Deployment approval.** Manual gate before prod (Jenkins `input`, GitHub Environments, ArgoCD sync); restricted to approver group.

**19. Blue-green.** Two identical envs (blue active, green idle); deploy to green; switch LB; instant rollback by switching back. Doubles infra cost.

**20. Canary.** Deploy new version to a small % of traffic; observe; gradually increase. Limits blast radius of bad deploys.

**21. Rolling.** Replace pods/instances incrementally (e.g., 25% at a time). Default for K8s Deployments.

**22. Feature flag.** Runtime toggle (LaunchDarkly, GrowthBook) decoupling deploy from release; enable feature for subset of users without redeploying.

**23. Branching strategy.** Convention for using branches (GitFlow, trunk-based, GitHub Flow) — affects merge cadence and release model.

**24. GitFlow.** Long-lived `develop`/`main` + feature/release/hotfix branches; complex, suits release-train models.

**25. Trunk-based.** Short-lived feature branches merging into trunk frequently (multiple times/day); requires strong tests + feature flags. Modern default.

**26. CI/CD speeds delivery.** Automation removes manual gates; small frequent changes; faster feedback; consistent envs; reliable rollback.

**27. Pipeline security.** Pinned tool versions, signed commits, secrets via vault, least-privilege CI tokens, SBOM generation, SCA + SAST + DAST + image scan, audit logging.

### Difficult

**28. CI/CD for microservices.** One pipeline per service; shared library for common steps; per-service Helm chart; promote via image digest; central gating dashboard; service-level deploy independence.

**29. CI/CD for K8s.** Build → push image → update Helm values / Kustomize manifest → apply or GitOps (ArgoCD/Flux) sync. Use Helm rollback or manifest revert for rollback.

**30. Implement rollback.** Keep previous artifact + manifest in registry; one-click rollback via pipeline parameter; auto-rollback on failed health check; for DB, design migrations to be backward compatible.

**31. Failed prod deploy.** Stop traffic shift / rollback immediately, declare incident, notify users, preserve logs, RCA, fix forward in next iteration only after issue understood.

**32. Secrets in CI/CD.** Vault / AWS Secrets Manager / GitHub OIDC → STS short-lived creds; never hardcode; mask in logs; rotate; least-privilege scope.

**33. Terraform in CI/CD.** PR runs `plan` + posts to PR; merge runs `apply` with manual gate; state in S3+DynamoDB; OIDC-based AWS auth; pre-commit hooks (fmt, validate, tflint, tfsec).

**34. Quality gates.** Unit test coverage threshold, SonarQube quality gate, mandatory code review approval, no critical lint issues.

**35. Security gates.** SCA (Snyk/Dependabot), SAST (Semgrep, SonarQube), secret scanning (gitleaks), image scan (Trivy), IaC scan (tfsec/checkov), DAST in staging.

**36. Reduce pipeline time.** Cache deps (Maven, npm, Docker layers), parallelize stages, smaller images, test sharding, skip unchanged services (Bazel/Nx), build matrix.

**37. Manage multiple envs.** Promotion model (dev → staging → prod) using same artifact; env-specific config (Helm values, ConfigMaps); separate IAM, separate state, separate creds per env.

**38. DB migrations.** Use migration tools (Flyway, Liquibase) run as pipeline step; design backward-compatible (expand-then-contract), separate schema and code deploys, automate rollback or forward-fix only.

**39. Enterprise DevOps.** Platform team provides golden paths (Backstage, internal CLI), self-service infra, central observability, paved-road CI templates, security-by-default, SLO-based delivery metrics.

---

## SECTION 18 — INFRASTRUCTURE AS CODE

### Easy

**1. IaC.** Defining and provisioning infra via code, version-controlled, repeatable.

**2. Why used.** Consistency, repeatability, version history, peer review, disaster recovery, multi-env parity.

**3. IaC tools.** Terraform, Pulumi, CloudFormation, ARM/Bicep, Ansible, Chef, Puppet, SaltStack, AWS CDK.

**4. Terraform.** Declarative IaC by HashiCorp using HCL.

**5. CloudFormation.** AWS-native declarative IaC using YAML/JSON templates.

**6. Ansible.** Agentless config management + provisioning tool using YAML playbooks (procedural-ish).

**7. Configuration management.** Maintaining the *configuration state* of existing servers (packages, files, services). Ansible/Chef/Puppet.

**8. Provisioning.** Creating the underlying infra (VMs, networks, DBs). Terraform/CloudFormation.

### Medium

**9. Terraform vs Ansible.** Terraform: provisioning infra, declarative, state file. Ansible: config management on existing nodes, procedural-declarative hybrid, no state. Often used together.

**10. Terraform vs CloudFormation.** TF: multi-cloud, larger module ecosystem. CF: AWS-only, native AWS service support, no state file (managed by AWS).

**11. When Ansible.** OS patching, package install, file/template management, app config, orchestrating multi-step deployments.

**12. When Terraform.** Cloud resource provisioning (VPCs, EKS, IAM, RDS, S3), multi-cloud, infra teardown.

**13. IaC benefits.** Repeatable, reviewable, auditable, fast DR, env parity, automation-friendly.

**14. Consistency.** Same code → same infra; eliminates snowflake servers; reduces "works in dev, broken in prod".

**15. DR via IaC.** Re-provision entire environment in a new region/account from code in minutes/hours.

**16. IaC in Git.** Same flow as app code: branches, PRs, reviews, CI checks, tagged releases.

**17. Review IaC.** PRs with `terraform plan` posted; static analysis (tflint, tfsec, checkov); senior approval for prod; OPA/Sentinel policy gates.

**18. Immutable infra.** Servers never modified after provisioning — replaced by new ones (AMI bake → ASG rotate). Eliminates drift.

**19. Idempotency.** Running the same code repeatedly produces the same end state, no matter the starting point.

### Difficult

**20. Multi-env IaC.** Modules + per-env root configs with separate state, separate IAM roles, parameterized via tfvars or Terragrunt.

**21. Multi-account.** AWS Organizations + assume-role provider config; one state per account/env; central pipeline can apply to all via OIDC.

**22. Prevent accidental delete.** `prevent_destroy`, IAM deny on `*Delete*` for prod, manual approvals, separate destroy pipeline, backups.

**23. Secrets in IaC.** External lookup (Vault, Secrets Manager) at apply time; mark `sensitive=true`; encrypt state; restrict state access.

**24. Test IaC.** `terraform validate`, `tflint`, `tfsec`/`checkov` static; `terratest` (Go) for integration tests against real cloud (use sandbox account); Kitchen-Terraform.

**25. Compliance via IaC.** OPA/Sentinel policies (no public S3, mandatory tags, MFA on IAM users); Service Control Policies in AWS Org; CIS benchmarks via Inspector / Config rules.

**26. IaC in CI/CD.** PR plan → review → apply on merge with approval gate; artifact = immutable plan file; OIDC creds; concurrent run prevention via locks.

**27. Manage drift.** Scheduled `terraform plan` in CI alerts on drift; periodic `apply` to reconcile; org-wide rule that all changes go through Git.

**28. Standardize modules.** Internal Terraform Registry, semantic versioning, READMEs, examples, automated testing of modules in CI, deprecation policy.

**29. Reusable modules.** Define interface clearly (variables/outputs); avoid hardcoding; one module = one logical concern; document inputs/outputs; pin provider versions.

---

## SECTION 19 — LINUX / INFRASTRUCTURE TROUBLESHOOTING

### Easy

**1. Linux.** Open-source Unix-like kernel + userland (distros: Ubuntu, RHEL, Debian, Alpine).

**2. Process.** A running instance of a program; has PID, parent PID, memory, file descriptors.

**3. Service.** A long-running background process (daemon) typically managed by systemd/init.

**4. SSH.** Secure Shell — encrypted remote terminal access, default port 22.

**5. Port.** TCP/UDP endpoint number identifying a service on a host (HTTP 80, HTTPS 443, SSH 22).

**6. DNS.** Domain Name System — resolves hostnames to IPs.

**7. Firewall.** Network filter (iptables, nftables, firewalld, ufw, cloud SG) controlling inbound/outbound traffic.

**8. File permission.** rwx for user/group/other (`-rwxr-xr--` = 754). Set with `chmod`.

**9. Root user.** UID 0; superuser; full access. Avoid login as root; use sudo.

**10. sudo.** Run a command as another user (typically root) with auditing.

**11. systemd.** Modern init/service manager — units (`.service`, `.timer`, `.socket`); `systemctl start/stop/status/enable`.

**12. cron.** Time-based job scheduler. Files in `/etc/cron.*` and `crontab -e` per user.

### Medium

**13. CPU usage.** `top`, `htop`, `mpstat 1`, `vmstat 1`, `pidstat 1`. `top` press `1` for per-CPU.

**14. Memory.** `free -h`, `vmstat 1`, `/proc/meminfo`. Working set: RSS in `top`.

**15. Disk.** `df -h` (mount points), `du -sh /path` (per directory), `iostat -xz 1` (IO).

**16. Running processes.** `ps -ef`, `ps aux`, `pgrep`, `pidof`. Tree: `pstree -p`.

**17. Open ports.** `ss -tulpn` (preferred), `netstat -tulpn`, `lsof -i`. `nc -zv host port` for connectivity test.

**18. Network connectivity.** `ping`, `traceroute`/`mtr`, `dig`, `curl -v`, `nc -zv`, `tcpdump -i any host X`.

**19. Service status.** `systemctl status nginx`, `journalctl -u nginx -n 100 --no-pager`.

**20. Restart service.** `systemctl restart nginx` (`reload` for graceful where supported).

**21. System logs.** `journalctl` (systemd), `/var/log/syslog`, `/var/log/messages`, `dmesg` (kernel ring buffer).

**22. App logs.** `/var/log/<app>/*`, container stdout (`docker logs`, `kubectl logs`), or app-defined paths.

**23. top vs htop.** Both real-time process viewers; htop adds color, scroll, mouse, tree mode, easier kill/renice. Functionally equivalent.

**24. df vs du.** `df` shows filesystem-level free space (from FS metadata). `du` walks the directory tree summing file sizes. They can disagree (deleted-but-open files).

**25. curl vs wget.** Both download URLs. curl: more protocols, scripting-friendly, prints to stdout, used for APIs. wget: recursive download, retries, default save-to-file.

**26. netstat vs ss.** `ss` is the modern replacement; faster (parses kernel structs directly), same/more options. Use ss.

**27. Disk full.** `df -h` find full FS → `du -sh /path/* | sort -h` drill down → identify large/old files; check deleted-but-open: `lsof | grep deleted`. Rotate/compress logs, expand FS.

**28. High CPU.** `top` (sort by %CPU), identify process, `pidstat -t -p <pid> 1` for thread breakdown, `perf top` or `strace -p` for hotspots, check load average.

**29. High memory.** `free -h`, `top` sort by %MEM, `ps aux --sort=-%mem | head`, check swap, OOM killer in `dmesg`.

**30. App not reachable.** Service running? Listening on expected port (`ss -tulpn`)? Firewall (iptables/SG)? DNS resolving correctly? Local from app side (`curl localhost`) vs remote (`curl host`)?

### Difficult

**31. Intermittent network.** Continuous `mtr` to find lossy hop, `tcpdump` capture during issue, dmesg for NIC errors, switch port stats, cloud VPC Flow Logs, cross-AZ vs in-AZ test.

**32. DNS failure.** `dig +trace name`, check `/etc/resolv.conf`, `nslookup` against alternate resolver, CoreDNS pods (if K8s), cache TTL, ndots config, search-domain expansion.

**33. High load average.** Load = runnable + uninterruptible sleep procs. Check `vmstat 1` (`b` column = blocked), iostat for IO wait, `top` for CPU vs IO bound, count of processes.

**34. Zombie processes.** Defunct in `ps` (Z state). Caused by parent not reaping; usually parent bug. Restart parent. If init (PID 1) is correct, kernel will reap.

**35. Permission issues.** `ls -la` to view, check user/group/SELinux/AppArmor (`getenforce`, `aa-status`), ACLs (`getfacl`), bind-mount root_squash on NFS, file capabilities (`getcap`).

**36. SSL certs.** `openssl s_client -connect host:443 -servername host` to inspect; check expiry, chain, SAN, hostname match. Renew via cert-manager / certbot.

**37. Slow app from Linux.** `top`/`vmstat` for CPU/IO/swap, `iostat` for disk latency, `ss -i` for TCP retransmits, `strace`/`perf` for syscalls, `tc qdisc` for traffic shaping.

**38. Disk hog.** `du -ah /path | sort -rh | head`, `ncdu /` interactive, `lsof | grep deleted` for held-open deletions.

**39. Port user.** `ss -tulpn | grep :8080` shows PID; `lsof -i :8080`.

**40. Analyze logs during outage.** `journalctl --since "10 min ago"`, time-correlate across hosts (NTP synced), grep for ERROR/WARN, parse access logs (awk by status), check for sudden traffic shift.

**41. Kernel-level.** `dmesg -T`, `/var/log/kern.log`, oops/panic stacks, OOM messages, hung tasks (`hung_task_panic`), kdump for post-mortem.

**42. Boot failure.** Single-user mode / rescue ISO; check `/etc/fstab`, `/boot`, kernel panic, GRUB config, disk failure (`smartctl`), missing module (initramfs).

**43. Log rotation.** `logrotate` (`/etc/logrotate.d/`); rotate by size/time, compress, retain N copies. systemd-journald has `SystemMaxUse=` / `MaxRetentionSec=`.

**44. Linux + Prometheus.** Node Exporter on each host; scrape via Prometheus; standard dashboards (Grafana 1860). Custom textfile collector for ad-hoc metrics.

---

## SECTION 20 — DOCKER / CONTAINERS

### Easy

**1. Docker.** Platform for building, distributing, and running OS-level virtualized containers using Linux namespaces and cgroups.

**2. Container.** Isolated process group with its own filesystem, network, PID namespace; lighter than a VM (shares host kernel).

**3. Image.** Read-only template (layered filesystem) used to create containers.

**4. Dockerfile.** Text recipe for building images: `FROM`, `RUN`, `COPY`, `CMD`, `ENTRYPOINT`, etc.

**5. Docker Hub.** Public container registry. `docker push/pull` default registry.

**6. Container registry.** Storage for images (Docker Hub, ECR, GCR, GHCR, ACR, Harbor, Nexus).

**7. Image vs container.** Image = blueprint (read-only); container = running instance (read-write top layer).

**8. Port mapping.** `-p 8080:80` maps host port 8080 to container port 80.

**9. Volume mounting.** `-v /host:/container` or named volumes — persist data outside container lifecycle.

**10. Docker Compose.** YAML-based multi-container orchestration for local/dev (`docker-compose up`).

### Medium

**11. Build image.** `docker build -t myapp:1.0 .` (uses Dockerfile in current dir).

**12. Run container.** `docker run -d --name web -p 80:80 myapp:1.0`.

**13. Container logs.** `docker logs -f <name>` or `kubectl logs <pod>` for K8s.

**14. Enter container.** `docker exec -it <name> /bin/sh` (or `bash` if installed).

**15. Stop & remove.** `docker stop <name> && docker rm <name>`. Remove image: `docker rmi <image>`.

**16. Layers.** Each Dockerfile instruction creates a layer; layers are cached and shared between images. Order instructions for cache hits (deps before code).

**17. Multi-stage build.** Multiple `FROM` in one Dockerfile; copy artifacts between stages; final image contains only what's needed.
```dockerfile
FROM golang AS build
COPY . .
RUN go build -o app
FROM alpine
COPY --from=build /app /app
ENTRYPOINT ["/app"]
```

**18. Reduce image size.** Multi-stage; small base (alpine, distroless, scratch); combine RUN to reduce layers; remove cache (`apt clean`); .dockerignore.

**19. Env vars.** `-e KEY=VALUE` or `--env-file file` or `ENV` in Dockerfile.

**20. Container-to-container.** Same Docker network (bridge/user-defined); resolve by container name. Docker Compose creates a default network. K8s: Service DNS.

**21. CMD vs ENTRYPOINT.** ENTRYPOINT defines the executable; CMD provides default args. With both, final command = ENTRYPOINT + CMD. Override CMD via `docker run img args`.

**22. COPY vs ADD.** COPY: simple file copy (preferred). ADD: also untars archives and supports URLs (avoid unless needed).

**23. Restart issues.** `docker logs <name>`, `docker inspect` for exit code, healthcheck failure, missing env/volume, OOM (check `docker stats`).

### Difficult

**24. Secure images.** Use minimal base, run as non-root (`USER 1000`), pin versions, scan with Trivy/Snyk, sign with Cosign, drop capabilities, read-only FS.

**25. Scan images.** `trivy image myapp:1.0`; integrate in CI; fail on HIGH/CRITICAL CVEs; tag and rebuild base regularly.

**26. Secrets in containers.** Mount as files via Docker secrets / K8s Secret volume; never bake into image; never `ENV` for sensitive values (visible in inspect/history); use external secret stores.

**27. Networking debug.** `docker network ls/inspect`, `nsenter` into container netns, `tcpdump -i any` from host, `docker exec` + `curl/ping/dig`. K8s: `kubectl exec` + `nslookup`, NetworkPolicy review.

**28. Monitor containers.** cAdvisor (built into kubelet) for resource metrics, Prometheus + Grafana, app metrics on `/metrics`, log shipping via Fluent Bit.

**29. Optimize builds.** BuildKit (`DOCKER_BUILDKIT=1`), layer caching (CI cache), `--cache-from` registry cache, multi-stage, parallel stages, smaller context (.dockerignore).

**30. Production images.** Distroless / Alpine base, non-root user, healthcheck, signal handling (PID 1 traps SIGTERM), structured logs, no secrets, image size minimized, CVE-scanned.

**31. High memory in container.** `docker stats`/`kubectl top pod`; profile app heap; check for memory leaks; verify limits set; check swap settings; ensure JVM `-XX:+UseContainerSupport`.

**32. Docker → Kubernetes.** Dockerfile builds image; image pushed to registry; Pod spec `image:` field references it; Deployment manages replicas; container runtime (containerd) on each node pulls and runs.

**33. Persistent data.** Volumes (Docker named volume) or bind mounts; in K8s: PV + PVC backed by EBS/EFS/NFS/CSI; StatefulSet for per-pod stable storage.

---

## SECTION 21 — MICROSERVICES VS MONOLITHIC ARCHITECTURE

### Easy

**1. Monolithic.** Single deployable unit containing all functionality, shared DB, single process.

**2. Microservices.** Many small, independently deployable services each owning a bounded domain and its data.

**3. Difference.** Monolith: one codebase/process, simple ops, hard to scale by team. Microservices: many services, complex ops, independent scaling/teams.

**4. Monolith benefits.** Simple to develop/deploy/debug; one DB transaction; faster initial velocity; less infra cost.

**5. Microservices benefits.** Independent deploys, polyglot tech, fault isolation, scale-by-component, team autonomy.

**6. Microservices disadvantages.** Network failures, distributed transactions, observability complexity, eventual consistency, ops overhead.

**7. Service-to-service.** Synchronous (HTTP/REST/gRPC) or asynchronous (messaging — Kafka, SQS, RabbitMQ).

**8. API Gateway.** Edge service handling routing, auth, rate limiting, request transformation in front of microservices (Kong, AWS API Gateway, Apigee, Spring Cloud Gateway).

### Medium

**9. Why migrate.** Scaling bottlenecks, deploy cadence per team, tech-stack diversification, fault isolation, large codebase becoming unmaintainable.

**10. Communication.** Sync (REST, gRPC) for request-response; Async (Kafka, NATS, SNS+SQS) for events and decoupling.

**11. Synchronous.** Caller waits for response. Simple but tightly coupled; failure cascades.

**12. Asynchronous.** Caller publishes event and continues; consumer processes later. Decouples failure domains; eventual consistency.

**13. Message queue.** FIFO buffer between producers and consumers (RabbitMQ, SQS); decoupling, retry, throttling.

**14. Service discovery.** Mechanism for services to find each other dynamically — DNS-based (K8s Services, Consul), client-side (Eureka), or via service mesh.

**15. Circuit breaker.** Pattern that stops calling a failing dependency for a window after N failures, preventing cascade. (Hystrix, Resilience4j, Istio outlier detection.)

**16. Distributed tracing.** Tracking a request as it flows across many services using a shared trace ID (OpenTelemetry, Jaeger, Zipkin).

**17. Why monitoring is harder.** N services, N×N call paths, failure can be anywhere, no single log file, cross-service trace context required.

**18. Troubleshoot microservices.** Trace ID → identify failing span → service logs for that trace_id → metrics for that service (RED) → upstream/downstream health.

**19. Common failures.** Network partitions, downstream timeouts, cascading failures, message-queue lag, schema mismatches, version skew during deploys.

**20. Eventual consistency.** Data is consistent across services *eventually*, not immediately. Consequence of distributed state with async updates.

### Difficult

**21. Monitor microservices platform.** Standardized metrics (RED), shared tracing, structured logs with `service.name` + `trace_id`, dependency map (service mesh), per-service SLOs, central dashboards.

**22. Trace cross-service.** OTel SDK propagates W3C `traceparent` header; each service creates child span; central trace backend reconstructs the tree.

**23. Identify failing service.** From trace tree, the lowest span with error status or excessive duration; cross-check with that service's RED metrics and recent deploys.

**24. Logging standards.** JSON format, fields: `timestamp`, `level`, `service`, `trace_id`, `span_id`, `request_id`, `user_id`, `message`. Library wrapper to enforce.

**25. Cascading failures.** Use circuit breakers, bulkheads, timeouts (always), retries with backoff + jitter, rate limiting, async where possible, capacity planning.

**26. Microservices alerts.** Per-service SLO-based: error rate, p99 latency, availability. Add saturation: queue depth, thread pool, DB conn pool. Avoid raw infra alerts.

**27. Migrate from monolith.** Strangler-fig pattern: extract one bounded context at a time, route traffic to new service via gateway, retire monolith piece. Don't rewrite all at once.

**28. Operational challenges.** More artifacts to deploy, distributed debugging, schema evolution, secret/config management at scale, security surface, network costs.

**29. Versioning.** Backward-compatible API changes (additive); semantic versioning; URL/header versioning (`v1`/`v2`); contract tests (Pact); deprecation policy.

**30. Reliability.** SLO-driven, redundancy at every tier, circuit breakers, retries, idempotent APIs, health checks, multi-AZ/region, chaos engineering.

---

## SECTION 22 — INCIDENT MANAGEMENT / ON-CALL SUPPORT

### Easy

**1. Incident management.** Structured process to detect, respond, mitigate, and learn from production issues.

**2. On-call support.** Rotation where engineers are responsible for responding to alerts/incidents 24×7 (or business hours).

**3. Severity.** Magnitude of impact (Sev1 = full outage, Sev2 = major degradation, Sev3 = minor, Sev4 = cosmetic).

**4. Priority.** Urgency of fix; often correlated with severity but distinct (Sev3 with regulatory deadline can be P1).

**5. SLA.** Service Level Agreement — contractual commitment with customer (e.g., 99.9% monthly uptime, response within 1h).

**6. Escalation.** Routing an unacknowledged or unresolved incident to next responder/manager based on time elapsed.

**7. Runbook.** Step-by-step guide for handling a known alert: diagnose, mitigate, escalate.

**8. Postmortem.** Document analyzing an incident: timeline, root cause, impact, actions, lessons (blameless culture).

**9. RCA.** Root Cause Analysis — investigating *why* an incident happened, not just what.

**10. Incident bridge.** Real-time call/chat where responders coordinate during a major incident.

### Medium

**11. Respond to alert.** Acknowledge → read description + runbook → check dashboard → confirm impact → mitigate (rollback/scale/failover) → notify → document.

**12. Steps in incident.** Detect → triage → declare severity → assemble responders → mitigate → recover → verify → close → postmortem.

**13. Decide severity.** Based on customer impact (count, geography), data loss risk, revenue impact, regulatory exposure, and agreed thresholds (severity matrix).

**14. Communicate.** Status page updates, Slack incident channel, exec broadcast for Sev1/2, customer comms via support, hourly cadence updates.

**15. Info during outage.** Timestamps, affected service/region, error patterns, recent deploys, related alerts, traffic levels, dashboard screenshots, hypothesis log.

**16. Perform RCA.** Build timeline → 5 Whys → identify trigger + contributing factors → categorize (code/config/capacity/external) → identify detection gaps and prevention.

**17. Prevent repeat.** Action items with owners + due dates, automated tests for the regression, runbook updates, alert improvements, capacity adjustments, design changes.

**18. Postmortem contents.** Summary, impact, timeline, root cause, contributing factors, lessons learned, action items (with owners and dates).

**19. Handle false alerts.** Tune thresholds, add `for:` window, use multi-condition, delete if non-actionable, periodic alert audit.

**20. Stakeholder comms.** Status page for customers, Slack #incidents for engineering, exec briefing for high impact, post-incident report; translate to business impact.

### Difficult

**21. Critical outage.** Declare incident, assign IC, gather responders in bridge, freeze deploys, mitigate first (rollback) before RCA, status updates every 15–30min, executive escalation, postmortem.

**22. Multi-system issue.** Trace ID + dependency map; identify earliest failing component (root vs symptom); log-time-correlation; engage all owning teams in bridge; structured RCA with timeline.

**23. No clear logs.** Traces (if any), system metrics anomalies, recent change log, network captures, API audit logs, customer reports for symptom; instrument better and wait for repro.

**24. Reduce MTTR.** Better dashboards, tested runbooks, alert-to-runbook links, automation (rollback button, auto-restart), training/game days, on-call drills.

**25. Better runbooks.** Live during incidents → updated post-incident; include exact commands, dashboards, escalation contacts, common false-alarm checks; tested quarterly.

**26. Improve alert quality.** SLO-based alerts, multi-burn-rate, deduplication, ownership tagging, monthly noise audit, kill alerts not actioned in 90d.

**27. Business impact comms.** Translate to revenue/customers/SLA breach; clear remediation ETA; honest, frequent updates; single owner of comms.

**28. Prioritize multiple incidents.** By customer impact + revenue exposure + data risk; assign separate IC per incident; pull in additional responders; comms for each separately.

**29. Recurring incidents.** Pattern analysis (trend dashboard); permanent fix prioritized over workarounds; root-cause class (config drift, capacity, flaky dep) addressed structurally.

**30. Lead bridge call.** As Incident Commander: maintain timeline, run roll call, assign tasks, control comms cadence, decide mitigation, prevent groupthink, declare resolution.

---

## SECTION 23 — SCRUM / AGILE / COMMUNICATION

### Easy

**1. Agile.** Iterative software-development approach valuing working software, customer collaboration, responding to change (Agile Manifesto).

**2. SCRUM.** A specific Agile framework — fixed-length sprints, defined roles (PO, SM, Dev), ceremonies (planning, standup, review, retro).

**3. Sprint.** Time-boxed iteration (1–4 weeks) producing a potentially shippable increment.

**4. Sprint planning.** Ceremony at sprint start — team commits to sprint backlog from prioritized product backlog.

**5. Daily standup.** 15-min sync: yesterday / today / blockers.

**6. Retrospective.** End-of-sprint review of process — what went well, what didn't, action items for next sprint.

**7. Backlog.** Prioritized list of work items maintained by the Product Owner.

**8. User story.** Small unit of work in user-value form: "As a <user>, I want <feature> so that <benefit>".

**9. Acceptance criteria.** Conditions defining when a story is "done"; testable.

**10. Story point.** Relative effort estimate (Fibonacci 1,2,3,5,8); not time, not 1:1 hours.

### Medium

**11. SCRUM workflow.** PO grooms backlog → sprint planning commits to sprint goal → daily standup tracks progress → review demos work → retro improves process → repeat.

**12. Role in planning.** Engineer: estimate stories, raise risks, clarify acceptance criteria, propose technical approach, commit to capacity.

**13. Priority changes.** New work goes to backlog; current sprint protected unless scope-swap (remove equivalent points). Escalate if frequent.

**14. Coordinate with multiple teams.** Shared roadmap, dependency tracking (Jira link issues), Scrum-of-Scrums, joint design reviews, clear API contracts.

**15. Stakeholder expectations.** Communicate scope/timeline/risks early; provide regular demos; honest progress updates; don't over-promise.

**16. Communicate technical risks.** Frame in business impact (delivery delay, customer impact); propose mitigation options with trade-offs; document in risk register.

**17. Document plans.** Confluence design doc with goals, non-goals, alternatives, decision, milestones, risks; lightweight RFCs for cross-team work.

**18. Support + sprint work.** Reserved capacity (e.g., 20%) for unplanned ops; rotation for who handles incidents this sprint; stretch goals beyond commitment.

### Difficult

**19. Conflicting requirements.** Surface trade-offs explicitly; escalate to PO/leadership for decision; document the chosen path and rejected alternatives.

**20. Influence without authority.** Lead with data, build alliances, frame in others' goals, deliver early wins, be the most reliable engineer in the room.

**21. Explain to non-technical.** Use analogies, focus on business impact, avoid jargon, visualize with diagrams, give one-line summary then details on demand.

**22. Delayed delivery.** Communicate early with new ETA + reason + mitigations; offer scope-cut alternatives; prevent recurrence (better estimation, smaller stories).

**23. Balance project + support.** Reserved support capacity, rotation, on-call handoff cadence, automate toil to reduce support load over time.

**24. Business escalation.** Acknowledge urgency, clarify the business problem (not solution), provide ETA + status updates, deliver fix, follow-up post-incident.

**25. Build trust as advisor.** Deep understanding of their domain, proactively flag risks, deliver consistently, transparent about uncertainty, be present in their forums.

---

## SECTION 24 — SCENARIO-BASED INTERVIEW QUESTIONS

### Easy

**1. Server CPU high.** `top`/`htop` to find process; check recent deploys, runaway loops, traffic spike; CW/Prometheus trend; mitigate by scaling, restart, or rollback.

**2. Disk full.** `df -h` confirm, `du -sh /*` drill down, `lsof | grep deleted` for held-open files; clean logs, expand FS, add to monitoring with disk-watermark alert.

**3. Service down.** `systemctl status <svc>`, `journalctl -u <svc>`, check config, dependencies (DB, network), recent change. Restart, then RCA.

**4. App slow.** Compare baseline (Grafana p95), check downstream latency (DB, external APIs), GC pauses, traffic spike, infra saturation; trace a slow request.

**5. No logs in Kibana.** Filebeat/Fluent Bit running? Index pattern matches? Time range correct? ES cluster healthy? Parse failures? ILM deleted index?

**6. Grafana no data.** Data source health? Time range? Query syntax? Variables resolved? Target up in Prometheus? Use Inspect → Query.

**7. Prometheus target down.** Check Targets page, last error message; curl `/metrics` directly; network/firewall; relabel rules dropping target.

**8. Pod not starting.** `kubectl describe pod` events; image pull, scheduling failure, probe failure, OOMKill, missing ConfigMap/Secret, PVC unbound.

**9. Jenkins build failed.** Read console; fix code or env; check tool version, dependency, agent disk space, secret expiry; rerun.

**10. Terraform unexpected changes.** `terraform plan` carefully; identify drift (manual change?), provider upgrade auto-changes, refactored module; reconcile by importing manual change or accepting plan.

### Medium

**11. App returning 500.** Logs for stack trace; APM error UI; trace ID; check downstream (DB connection, external API); recent deploy; scale or rollback.

**12. CrashLoopBackOff.** `describe pod`, `logs -p`; check OOMKilled (limits), liveness probe, missing config, app exception, image entrypoint failure.

**13. Prometheus not scraping.** Check Targets, scrape errors, network/firewall, relabel rules, target's `/metrics` reachable, scrape interval too short, server overloaded.

**14. Alertmanager not sending.** Check AM logs, route matchers, receiver config (Slack token / SMTP), inhibition rules silencing, silence active, network egress, AM cluster health.

**15. ES cluster yellow.** Unassigned replicas; `_cluster/allocation/explain`; check disk watermark, node count vs replica count, JVM, recent index settings.

**16. Jenkins agent offline.** Check agent process, network to controller, JNLP port, secret mismatch, disk full, JVM crash; reconnect.

**17. Terraform state lock stuck.** Confirm no other apply running; `terraform force-unlock <id>` carefully; for DynamoDB lock, manual delete after verification.

**18. EC2 unreachable.** SG/NACL allows? Public IP/route? OS firewall? Service listening? Status checks in console; SSM Session Manager as fallback.

**19. ALB 502.** Target health check status; idle timeout vs backend; backend returning malformed HTTP; SG between ALB and target; check target logs.

**20. Logs delayed in Kibana.** Filebeat/Logstash queue lag, ES indexing pressure (pending tasks), index rollover, network throughput, downstream backpressure.

**21. Grafana slow.** Heavy queries (high cardinality), too many panels, low refresh interval, recording rules missing, big time ranges. Inspect query duration.

**22. K8s service not reachable.** Endpoints populated? Selector matches pod labels? Probes passing? NetworkPolicy? kube-proxy mode? CoreDNS.

**23. Latency spike.** Recent deploy? Traffic surge? Downstream latency? GC pauses? Infra saturation? Compare metrics window-over-window.

**24. CW alarm didn't trigger.** Metric data missing (treat-missing-as setting), threshold logic, evaluation periods, alarm in INSUFFICIENT_DATA, action enabled, SNS topic permission.

### Difficult

**25. Outage with no alerts.** Coverage gap → audit alerts vs SLOs → add missing alerts. Investigate via metrics/logs/traces for the incident window; cross-correlate deploy timeline; postmortem with detection-improvement actions.

**26. Slow only for some users.** Likely a partition: region, tenant, feature flag, A/B variant, DB shard, cache key. Filter logs/traces by user attribute; check shard/replica health; recent rollout %.

**27. Incomplete traces.** Sampling drop, propagation broken (header missing across an HTTP client), Collector batch dropping, exporter errors, async boundary not propagating context.

**28. Diverging timelines.** NTP drift on hosts; verify clock sync; use server-side ingest timestamps consistently; correlate via trace_id rather than timestamp.

**29. Prometheus high cardinality.** `topk` on series count by name; identify offending labels; drop via `metric_relabel_configs`; coach team on cardinality; recompute as recording rule with reduced labels.

**30. ES storage growth.** Identify hot indices (`_cat/indices`); excessive log verbosity; misconfigured shards; mapping explosion; tighten ILM, drop noisy logs at source, add Kafka buffer with TTL.

**31. Cluster healthy but app down.** App-level issue: DB connection exhausted, downstream hard-down, queue backed up, deadlock, config flag, secret expiry. APM trace + app logs.

**32. Jenkins deployed wrong version.** Audit: pipeline parameters, source branch, image tag resolution; check who triggered; rollback immediately; add immutable tags + approval gates + artifact provenance.

**33. TF changed manual resources.** Drift between state and reality. Decide: import manual change into HCL (preserve), or apply TF version (overwrite). Add ChatOps alert on drift; enforce no-manual-change policy.

**34. AWS cost spike.** Cost Explorer by service/account/tag; unusual NAT data transfer, S3 storage growth, untagged dev resources, runaway autoscaling, expensive instance type, forgotten EBS/Snapshot. Tag, alert, kill noise.

**35. Multiple alerts at once.** Use Alertmanager grouping + inhibition; identify root alert (lowest in dependency graph); silence noisy children; bridge call to coordinate.

**36. Business says down, dashboards green.** Coverage gap. Reproduce as a real user; check synthetic/RUM; check geographic edge (CDN, DNS, ISP); check feature flag for that segment; check auth/session path.

**37. Intermittent customer errors, unclear logs.** Need RUM + better instrumentation; capture client-side errors (Sentry); add server-side request_id customer can quote; distributed trace for that ID.

**38. End-to-end monitoring for new app.** Define SLIs/SLOs from user journey → instrument with OTel SDK (metrics, logs, traces) → standard log fields → Prometheus + Grafana dashboards (RED) → Alertmanager rules → runbooks → synthetic checks → on-call rotation.

**39. Migrate ELK → OTel observability.** Inventory pipelines; deploy OTel Collector alongside Logstash/Filebeat; route traces+metrics+logs via OTel; dual-output to ES during transition; build new Grafana dashboards over Tempo/Loki/Prometheus; cut over per service; decommission old stack.

**40. Single pane of glass.** Grafana with multiple data sources (Prometheus, Loki/ES, Tempo/Jaeger, CW). Standard dashboards templated by service variable; cross-link panels by trace_id; folder-by-team. Add executive-level KPI overlay on top.

---

*End of answer sheet. Personal/biographical questions in Section A and Section 1 are intentionally left for you to draft in your own words using your real project specifics.*
