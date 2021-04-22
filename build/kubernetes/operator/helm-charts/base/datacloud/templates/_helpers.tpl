{{/*
Return the proper image name for the flat collector
*/}}
{{- define "datacloud.flatImage" -}}
{{- $repository := .Values.flat.image.repository | default .Values.image.repository -}}
{{- $tag := .Values.flat.image.tag | default .Values.image.tag | toString -}}
{{- $name := .Values.flat.image.name -}}
{{/*
Helm 2.11 supports the assignment of a value to a variable defined in a different scope,
but Helm 2.9 and 2.10 doesn't support it, so we need to implement this if-else logic.
Also, we can't use a single if because lazy evaluation is not an option
*/}}
{{- if .Values.global }}
    {{- if and .Values.global.repository .Values.global.tag (eq $repository "turbonomic") (eq $tag "latest") }}
        {{- printf "%s/%s:%s" .Values.global.repository $name .Values.global.tag -}}
    {{- else -}}
        {{- printf "%s/%s:%s" $repository $name $tag -}}
    {{- end -}}
{{- else -}}
    {{- printf "%s/%s:%s" $repository $name $tag -}}
{{- end -}}
{{- end -}}

{{/*
Return the proper image name for the graph collector
*/}}
{{- define "datacloud.graphImage" -}}
{{- $repository := .Values.graph.image.repository | default .Values.image.repository -}}
{{- $tag := .Values.graph.image.tag | default .Values.image.tag | toString -}}
{{- $name := .Values.graph.image.name -}}
{{/*
Helm 2.11 supports the assignment of a value to a variable defined in a different scope,
but Helm 2.9 and 2.10 doesn't support it, so we need to implement this if-else logic.
Also, we can't use a single if because lazy evaluation is not an option
*/}}
{{- if .Values.global }}
    {{- if and .Values.global.repository .Values.global.tag (eq $repository "turbonomic") (eq $tag "latest") }}
        {{- printf "%s/%s:%s" .Values.global.repository $name .Values.global.tag -}}
    {{- else -}}
        {{- printf "%s/%s:%s" $repository $name $tag -}}
    {{- end -}}
{{- else -}}
    {{- printf "%s/%s:%s" $repository $name $tag -}}
{{- end -}}
{{- end -}}

{{- define "datacloud.host" -}}
{{- .Values.host | default .Values.sevone.transport.settings.DE_HOST -}}
{{- end -}}

{{- define "datacloud.projectID" -}}
{{- $creds := .Values.credentials | default dict -}}
{{- $creds.project_id | default .Values.sevone.transport.settings.DE_PROJECT_ID -}}
{{- end -}}

{{- define "datacloud.authType" -}}
{{- $creds := .Values.credentials | default dict -}}
{{- if $creds.type -}}
{{- $creds.type -}}
{{- else if .Values.sevone.service_account_credentials_secret -}}
service_account
{{- else -}}
apikey
{{- end -}}
{{- end -}}

{{- define "datacloud.apikeySecret" -}}
{{- $creds := .Values.credentials | default dict -}}
{{- $creds.secret_name | default .Values.sevone.transport.sevone_auth_secret -}}
{{- end -}}

{{- define "datacloud.apikeySecretKey" -}}
{{- $creds := .Values.credentials | default dict -}}
{{- $creds.secret_key | default "SEVONE_GRPC_TOKEN" -}}
{{- end -}}

{{- define "datacloud.saSecret" -}}
{{- $creds := .Values.credentials | default dict -}}
{{- $creds.secret_name | default .Values.sevone.service_account_credentials_secret -}}
{{- end -}}

{{- define "datacloud.saSecretKey" -}}
{{- $creds := .Values.credentials | default dict -}}
{{- $creds.secret_key | default "credentials.json" -}}
{{- end -}}

{{- define "datacloud.env" -}}
{{- merge .Values.sevone.env .Values.env | toYaml -}}
{{- end -}}
