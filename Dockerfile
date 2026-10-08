# syntax=docker/dockerfile:1

# Versi Python dan suite Debian dikunci, bukan `python:3.12`: tag mengambang
# berganti isi diam-diam, dan image yang lolos uji hari ini belum tentu sama
# dengan yang dibangun bulan depan.
ARG PYTHON_IMAGE=python:3.12.15-slim-trixie

# --- Tahap 1: memasang paket -------------------------------------------------
FROM ${PYTHON_IMAGE} AS build

ENV PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:${PATH}"

# Dependensi dulu, dari file kunci.
#   --require-hashes   paket yang isinya tidak cocok dengan sidik jarinya ditolak
#   --only-binary      tidak ada kode pihak ketiga yang dijalankan saat build,
#                      kecuali satu paket yang memang tidak menyediakan wheel
COPY requirements.lock /src/requirements.lock
RUN pip install --require-hashes --only-binary=:all: --no-binary=protobuf3-to-dict \
        -r /src/requirements.lock

# Baru bridge-nya sendiri. --no-deps: tidak ada yang boleh ikut terpasang di
# luar file kunci.
COPY pyproject.toml README.md LICENSE /src/
COPY src /src/src
RUN pip install --no-deps /src

# --- Tahap 2: image yang dijalankan ------------------------------------------
FROM ${PYTHON_IMAGE}

LABEL org.opencontainers.image.title="thrift-live-bridge" \
      org.opencontainers.image.licenses="AGPL-3.0-only"

# Bridge memproses data dari luar tanpa henti, jadi tidak dijalankan sebagai
# root. /data (antrian lokal) satu-satunya tempat ia boleh menulis.
RUN useradd --uid 10001 --user-group --no-create-home --shell /usr/sbin/nologin bridge \
    && install -d -o bridge -g bridge -m 0700 /data \
    && install -d -m 0755 /config

COPY --from=build /opt/venv /opt/venv

# BRIDGE_SETTINGS dan BRIDGE_DATA_DIR menunjuk dua tempat yang dipasang dari
# luar: file setelan (hanya-baca) dan volume antrian. API key tidak ada di
# image; ia diberikan lewat environment saat container dijalankan.
ENV PATH="/opt/venv/bin:${PATH}" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    BRIDGE_SETTINGS=/config/clients.toml \
    BRIDGE_DATA_DIR=/data

USER bridge
WORKDIR /data
VOLUME /data

# Bentuk daftar (bukan teks): bridge menjadi proses nomor 1 dan menerima
# sendiri tanda berhenti dari `docker stop`, lalu memutus sambungan dengan rapi.
ENTRYPOINT ["thrift-live-bridge"]
CMD ["run"]
