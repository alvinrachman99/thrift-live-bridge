# thrift-live-bridge

Jembatan antara TikTok LIVE dan antrian order di Odoo. Saat host menyematkan
(pin) komentar pemenang di siaran, bridge membaca event itu dan mengirimkannya
ke Odoo sebagai satu baris antrian. Order baru dibuat di Odoo, oleh admin.

**Status: sedang dibangun.** Yang sudah ada: antrian lokal, pengirim ke Odoo,
pembaca siaran, banyak client dalam satu proses, laporan status ke Odoo, ping
ke layanan pemantau, dan kemasan Docker.

## Rancangan

```
TikTok LIVE ──► bridge ──► Odoo (module thrift_live)
```

- Bridge hanya **membaca** siaran. Ia tidak mengirim komentar dan tidak login
  sebagai akun TikTok mana pun.
- Di Odoo, bridge memakai user khusus yang tidak punya hak membaca customer
  maupun order. Yang disediakan untuknya hanya pintu masuk antrian: membuka
  sesi, menutup sesi, dan mengirim baris.
- Data penonton yang disimpan bridge hanyalah antrian sementara miliknya
  sendiri, untuk menahan kiriman saat Odoo tidak bisa dihubungi.

## Mengikuti siaran

- **Menunggu:** bridge memeriksa tiap menit apakah akunnya sedang siaran.
- **Sesi dibuka** saat tersambung, dan **ditutup** saat host mengakhiri siaran,
  atau saat sambungan putus dan akunnya dipastikan sudah tidak siaran. Putus
  saja bukan alasan menutup.
- **Yang dikirim:** komentar yang disematkan. Pin yang dilepas, dan pin berisi
  hadiah atau pesan lain, diabaikan.
- **Yang dibawa dari tiap pin hanya yang dipakai:** ID pin, waktu, ID akun,
  username, nama tampilan, dan teks komentar. Event aslinya memuat profil
  lengkap penonton (bio, foto, statistik); semua itu ditinggal.
- **Sambungan putus:** dicoba lagi setelah 5 detik, berlipat sampai 60 detik.
  Setelah 10 kali gagal berturut-turut, satu percobaan tiap 10 menit. Selama
  terputus, pin tidak terbaca.
- **Bridge dihentikan:** sesi tidak ditutup. Bridge yang dinyalakan lagi
  melanjutkan siaran yang sama, atau menutup sesinya kalau siaran itu ternyata
  sudah berakhir.

## Banyak client

Satu proses melayani semua client di file setelan. Tiap client punya antrian,
pengirim, pembaca, dan API key sendiri; yang dipakai bersama hanya key sign
server.

- Client yang setelannya belum lengkap (key atau akunnya kosong) dilewati
  dengan pesan galat; yang lain tetap jalan.
- Client yang berhenti karena galat tak terduga dinyalakan lagi sendiri,
  dengan jeda 5 detik yang berlipat sampai 60 detik. Client lain tidak ikut
  berhenti.
- Baris log diawali kode client-nya, misalnya `[k7m2]`.

## Laporan dan pemantauan

Dua tanda hidup, untuk dua pihak yang berbeda:

- **Ke Odoo, per akun** — tiap menit dan tiap keadaannya berubah, bridge
  melaporkan: menunggu siaran, tersambung, atau gagal menyambung beserta
  alasannya, dan kapan pin terakhir diterima. Odoo menampilkannya untuk admin,
  dan menandai bridge "tidak terdengar" kalau laporannya berhenti lebih dari
  tiga menit. Laporan ini tidak lewat antrian lokal: kalau Odoo tidak bisa
  dihubungi, laporannya dilewati.
- **Ke layanan pemantau, per proses** — satu ping tiap menit ke alamat di
  `BRIDGE_HEALTHCHECK_URL` (misalnya healthchecks.io). Ping yang berhenti
  berarti prosesnya mati atau macet, dan pengelola server diberi tahu.

## Tidak hilang, tidak dobel

Setiap kejadian dari siaran ditulis dulu ke antrian lokal (satu file SQLite per
client), baru dikirim ke Odoo. Kalau Odoo mati, jaringan putus, atau bridge
dihentikan di tengah siaran, kirimannya menunggu dan terkirim saat keadaan
pulih. Urutannya dijaga.

Kiriman ditandai sampai hanya setelah Odoo menjawab. Kalau jawabannya hilang
di jalan, kiriman itu dikirim lagi, dan Odoo melewati kembarannya karena pin
dikenali dari ID pesannya.

| Jawaban Odoo | Yang dilakukan bridge |
|---|---|
| 200 | Ditandai sampai |
| 422 (isi kiriman ditolak) | Dicoba lagi; setelah tiga kali ditolak, disisihkan supaya tidak menyumbat antrian. Tidak dihapus |
| Lainnya: Odoo mati, timeout, key ditolak, database tidak ditemukan | Ditahan dan dicoba lagi, dengan jeda yang makin panjang sampai satu menit |

## Menjalankan

Butuh Python 3.12 ke atas.

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e .
cp clients.example.toml clients.toml     # lalu sesuaikan
python -m unittest discover -s tests     # harus berakhir dengan OK
```

```bash
export EULER_API_KEY=...                 # key sign server; jangan ditulis ke file yang ikut git
export BRIDGE_HEALTHCHECK_URL=...        # alamat ping pemantau (boleh kosong); juga rahasia
thrift-live-bridge run                   # semua client; Ctrl+C untuk berhenti
thrift-live-bridge --client k7m2 run     # satu client saja
thrift-live-bridge --client k7m2 run --tiktok akun   # ikuti akun lain, untuk menguji
```

Bagian-bagiannya juga bisa dijalankan sendiri:

```bash
thrift-live-bridge status                # isi antrian lokal semua client
thrift-live-bridge send                  # kirim terus-menerus; Ctrl+C untuk berhenti
thrift-live-bridge send --once           # kirim yang ada sekarang, lalu berhenti
thrift-live-bridge requeue               # kembalikan kiriman yang disisihkan
```

Perintah `fake` menaruh kiriman buatan ke antrian, untuk menguji tanpa TikTok:

```bash
thrift-live-bridge fake open --ref T1
thrift-live-bridge fake pin --ref T1 --nickname "Budi" --comment "kemeja 35"
thrift-live-bridge fake close --ref T1
```

## Docker

```bash
docker build -t thrift-live-bridge:0.1.0 .
docker run --rm --env-file .env \
  -v "$PWD/clients.toml:/config/clients.toml:ro" \
  -v bridge-data:/data \
  thrift-live-bridge:0.1.0
```

- Image dibangun dari `requirements.lock`, bukan dari `pyproject.toml`: versi
  persis tiap paket, dan paket yang isinya tidak cocok dengan sidik jarinya
  ditolak. Cara membuat ulang file itu tertulis di baris-baris awalnya.
- Container berjalan sebagai user biasa, bukan root. Satu-satunya tempat ia
  menulis adalah `/data`, antrian lokalnya.
- Tiga hal datang dari luar image: file setelan di `/config/clients.toml`
  (dipasang hanya-baca), volume `/data`, dan API key lewat environment.
- `docker stop` menghentikannya dengan rapi: sambungan ke siaran diputus, dan
  sesi tidak ditutup.

## Rahasia

Repo ini tidak memuat rahasia. API key dibaca dari environment saat program
dijalankan; file setelan hanya menyebut nama variabelnya. File `.env`,
`clients.toml`, dan folder antrian ditolak `.gitignore`.

## Lisensi

[AGPL-3.0](LICENSE). Bridge ini memakai library `TikTokLive`; lisensi library
itu mensyaratkan AGPL untuk pemakaian pada layanan yang di-hosting.
