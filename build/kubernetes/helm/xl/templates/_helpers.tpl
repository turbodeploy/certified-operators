{{/* vim: set filetype=mustache: */}}
{{/*
Expand the name of the chart.
*/}}
{{- define "xl.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "xl.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "xl.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
  Set the XL component JVM environment params, based on any custom settings from the custom resource file.
  * If .Values.debug or .Values.global.debug are set, then JAVA_DEBUG="true" will be set in the env vars.
  (the java component will interpret this as a signal that the debugging runtime options should be added
  to the command line)
  * .Values.javaDebugOptions or .Values.global.javaDebugOptions will be passed as JAVA_DEBUG_OPTS
  * .Values.javaMaxRAMPercentage or .Values.global.javaMaxRAMPercentage will be passed as JAVA_MAX_RAM_PCT
  * .Values.javaComponentOptions will be passed as JAVA_COMPONENT_OPTS
  * .Values.javaOptions will be passed as JAVA_OPTS. This will completely override the default set of JVM
  runtime options.
*/}}
{{- define "java.setJVMEnvironmentOptions" }}
    {{- if or .Values.global.debug .Values.debug }}
        - name: JAVA_DEBUG
          value: "true"
    {{- end }}
    {{- if or .Values.global.javaDebugOptions .Values.javaDebugOptions }}
        - name: JAVA_DEBUG_OPTS
          value: {{ coalesce .Values.javaDebugOptions .Values.global.javaDebugOptions "" }}
    {{- end }}
    {{- if or .Values.global.javaMaxRAMPercentage .Values.javaMaxRAMPercentage }}
        - name: JAVA_MAX_RAM_PCT
          value: {{ coalesce .Values.javaMaxRAMPercentage .Values.global.javaMaxRAMPercentage "" | quote }}
    {{- end }}
    {{- if .Values.javaComponentOptions }}
        - name: JAVA_COMPONENT_OPTS
          value: {{ .Values.javaComponentOptions }}
    {{- end }}
    {{- if or .Values.global.javaBaseOptions .Values.javaBaseOptions }}
        - name: JAVA_BASE_OPTS
          value: {{ coalesce .Values.javaBaseOptions .Values.global.javaBaseOptions "" }}
    {{- end }}
    {{- if .Values.javaOptions }}
        - name: JAVA_OPTS
          value: {{ .Values.javaOptions }}
    {{- end }}
{{- end }}
