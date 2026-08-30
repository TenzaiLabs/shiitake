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
    - name: SHIITAKE_DEFAULT_WORKDIR
      value: {{ $.Values.defaultWorkdir | quote }}
    - name: SHIITAKE_AUTH_TOKEN
      value: {{ $.Values.authToken | quote }}
    - name: SHIITAKE_CAPTURE_ROOT
      value: {{ $.Values.captureRoot | quote }}
    - name: SHIITAKE_MIN_READY_WORKERS
      value: {{ $.Values.server.minReadyWorkers | quote }}
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
    - name: SHIITAKE_DISPATCH_URL
      value: {{ .dispatchUrl | quote }}
    - name: SHIITAKE_DISPATCH_TOKEN
      value: {{ $.Values.dispatchToken | quote }}
    - name: SHIITAKE_CONTAINER_NAME
      value: {{ .name | quote }}
    - name: SHIITAKE_CAPTURE_ROOT
      value: {{ $.Values.captureRoot | quote }}
    - name: SHIITAKE_RESET_PATHS
      value: {{ $.Values.worker.resetPaths | quote }}
    - name: SHIITAKE_RESTART_AFTER
      value: {{ $.Values.worker.restartAfter | quote }}
    - name: POD_NAME
      valueFrom:
        fieldRef:
          fieldPath: metadata.name
    - name: POD_NAMESPACE
      valueFrom:
        fieldRef:
          fieldPath: metadata.namespace
  resources:
    {{- toYaml $.Values.worker.resources | nindent 4 }}
  volumeMounts:
    - name: capture
      mountPath: {{ $.Values.captureRoot }}
{{- end -}}

{{/*
The capture volume, mounted at the same path by the server and every worker. Two
pods can't share an emptyDir; hostPath is enough for single-node k3d, but a real
multi-node deployment needs a ReadWriteMany volume.
*/}}
{{- define "shiitake.captureVolume" -}}
{{- $ := .root -}}
- name: capture
  {{- if eq $.Values.topology "two-pod" }}
  hostPath:
    path: {{ $.Values.capture.hostPath | quote }}
    type: DirectoryOrCreate
  {{- else }}
  emptyDir: {}
  {{- end }}
{{- end -}}
