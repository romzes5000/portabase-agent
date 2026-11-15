# oxem/stable

`STABLE` — тег образа, на который указывает `ghcr.io/romzes5000/portabase-agent:oxem-stable`.
Push в эту ветку переставляет тег (`.github/workflows/promote-stable.yml`).
Процедура выпуска и отката — `tools/portabase-agent-updater/README.md` в romans-workspace.

`MIRROR` — апстримные теги `ghcr.io/portabase/agent`, которые копируются в наш GHCR под тем же тегом (`.github/workflows/mirror-upstream.yml`). Чистый апстрим выпускается так: тег в `MIRROR` → канарейка fml → тот же тег в `STABLE`.
