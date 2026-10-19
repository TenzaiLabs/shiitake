{{/*
Container and volume partials shared by both topologies, so the two can't drift.
Every partial takes a dict whose `root` key is the chart context (`.`).
*/}}

{{/*
The shiitake-server container. `dispatchHost` is loopback when the workers share
this pod's netns, `0.0.0.0` when they arrive through a Service; the listener is
bearer-authenticated either way.
*/}}
{{- define "shiitake.serverContainer" -}}
{{- $ := .root -}}
- name: server
  image: {{ $.Values.server.image }}
  imagePullPolicy: IfNotPresent
  ports:
    - name: api
      containerPort: {{ $.Values.server.port }}
    - name: dispatch
      containerPort: {{ $.Values.server.dispatchPort }}
  env:
    - name: SHIITAKE_HOST
      value: "0.0.0.0"
    - name: SHIITAKE_PORT
      value: {{ $.Values.server.port | quote }}
    - name: SHIITAKE_DISPATCH_HOST
      value: {{ .dispatchHost | quote }}
    - name: SHIITAKE_DISPATCH_PORT
      value: {{ $.Values.server.dispatchPort | quote }}
    - name: SHIITAKE_DISPATCH_TOKEN
      value: {{ $.Values.dispatchToken | quote }}
    {{- if $.Values.workerProfiles }}
    {{- $profileTokens := dict }}
    {{- range $profile := $.Values.workerProfiles }}
    {{- $_ := set $profileTokens $profile.name (required "workerProfiles[].dispatchToken is required" $profile.dispatchToken) }}
    {{- end }}
    - name: SHIITAKE_PROFILE_TOKENS
      value: {{ $profileTokens | toJson | quote }}
    {{- end }}
    - name: SHIITAKE_DEFAULT_WORKDIR
      value: {{ $.Values.defaultWorkdir | quote }}
    - name: SHIITAKE_AUTH_TOKEN
      value: {{ $.Values.authToken | quote }}
    - name: SHIITAKE_CAPTURE_ROOT
      value: {{ $.Values.captureRoot | quote }}
    - name: SHIITAKE_MIN_READY_WORKERS
      value: {{ $.Values.server.minReadyWorkers | quote }}
    - name: SHIITAKE_PTY_KEEPALIVE_SECS
      value: {{ $.Values.server.ptyKeepaliveSecs | quote }}
    {{- if $.Values.otel.enabled }}
    - name: OTEL_EXPORTER_OTLP_ENDPOINT
      value: "http://{{ $.Release.Name }}-otel:4318"
    - name: OTEL_METRIC_EXPORT_INTERVAL
      value: {{ $.Values.otel.metricExportIntervalMs | quote }}
    {{- end }}
    - name: POD_NAME
      valueFrom:
        fieldRef:
          fieldPath: metadata.name
    - name: POD_NAMESPACE
      valueFrom:
        fieldRef:
          fieldPath: metadata.namespace
  # Readiness gates traffic on the pool (/ready is 503 until
  # minReadyWorkers workers have registered); liveness only asks
  # whether the process is up, so an empty pool never restarts it.
  readinessProbe:
    httpGet:
      path: /api/v1/ready
      port: {{ $.Values.server.port }}
    # No initialDelaySeconds: the server binds in milliseconds and
    # /ready is 503 until the pool fills, so probing immediately and
    # often is what makes the pod ready the instant it can serve.
    periodSeconds: 1
  livenessProbe:
    httpGet:
      path: /api/v1/health
      port: {{ $.Values.server.port }}
    periodSeconds: 10
  resources:
    {{- toYaml $.Values.server.resources | nindent 4 }}
  volumeMounts:
    - name: capture
      mountPath: {{ $.Values.captureRoot }}
{{- end -}}

{{/*
A shiitake-worker container. Args besides `root`:
  name                 container name.
  workerId             literal id, or omitted to take the pod name (unique per
                       replica, which a Deployment of worker pods needs).
  dispatchUrl          full WS URL of the dispatcher.
  perContainerRestart  emit `restartPolicy: Always` on the container; only
                       needed when the worker is one container of a bigger pod.

POD_NAME / POD_NAMESPACE / SHIITAKE_CONTAINER_NAME ride on the worker's Hello so
the OOM probe queries its pod, not the server's.
*/}}
{{- define "shiitake.workerContainer" -}}
{{- $ := .root -}}
- name: {{ .name }}
  image: {{ $.Values.worker.image }}
  imagePullPolicy: IfNotPresent
  {{- if .perContainerRestart }}
  # Recycle each worker independently on its SHIITAKE_RESTART_AFTER quota
  # (ContainerRestartRules; k8s >= 1.35). A worker pod needs none of this.
  restartPolicy: Always
  {{- end }}
  env:
    - name: SHIITAKE_WORKER_ID
      {{- if .workerId }}
      value: {{ .workerId | quote }}
      {{- else }}
      valueFrom:
        fieldRef:
          fieldPath: metadata.name
      {{- end }}
    - name: SHIITAKE_PROFILE
      value: {{ .profile | quote }}
    - name: SHIITAKE_DISPATCH_URL
      value: {{ .dispatchUrl | quote }}
    - name: SHIITAKE_DISPATCH_TOKEN
      value: {{ .dispatchToken | default $.Values.dispatchToken | quote }}
    - name: SHIITAKE_CONTAINER_NAME
      value: {{ .name | quote }}
    - name: SHIITAKE_CAPTURE_ROOT
      value: {{ $.Values.workerCaptureRoot | quote }}
    - name: SHIITAKE_RESET_PATHS
      value: {{ $.Values.worker.resetPaths | quote }}
    - name: SHIITAKE_RESTART_AFTER
      value: {{ $.Values.worker.restartAfter | quote }}
    - name: SHIITAKE_HOME_ROOT
      value: {{ $.Values.worker.homeRoot | quote }}
    - name: SHIITAKE_LEASE_TIMEOUT
      value: {{ $.Values.worker.leaseTimeout | quote }}
    - name: POD_NAME
      valueFrom:
        fieldRef:
          fieldPath: metadata.name
    - name: POD_NAMESPACE
      valueFrom:
        fieldRef:
          fieldPath: metadata.namespace
  resources:
    {{- toYaml .resources | nindent 4 }}
  {{- if .sharedFolder }}
  volumeMounts:
    - name: shared-folder
      mountPath: {{ $.Values.sharedFolder.mountPath }}
  {{- end }}
{{- end -}}

{{/*
The capture volume belongs only to the server. Workers stream output over their
dispatch sockets and do not need a shared capture mount.
*/}}
{{- define "shiitake.captureVolume" -}}
{{- $ := .root -}}
- name: capture
  emptyDir: {}
{{- end -}}

{{- define "shiitake.sharedFolderVolume" -}}
{{- $ := .root -}}
- name: shared-folder
  {{- if eq $.Values.topology "two-pod" }}
  hostPath:
    path: {{ $.Values.sharedFolder.hostPath | quote }}
    type: DirectoryOrCreate
  {{- else }}
  emptyDir: {}
  {{- end }}
{{- end -}}
