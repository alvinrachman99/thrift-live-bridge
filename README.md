# thrift-live-bridge

Jembatan antara TikTok LIVE dan antrian order di Odoo. Saat host menyematkan
(pin) komentar pemenang di siaran, bridge membaca event itu dan mengirimkannya
ke Odoo sebagai satu baris antrian. Order baru dibuat di Odoo, oleh admin.

**Status: sedang dibangun.** Yang sudah ada baru antrian lokal dan pengirim
ke Odoo. Pembaca siaran TikTok belum ada.

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
thrift-live-bridge status                # isi antrian lokal
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

## Rahasia

Repo ini tidak memuat rahasia. API key dibaca dari environment saat program
dijalankan; file setelan hanya menyebut nama variabelnya. File `.env`,
`clients.toml`, dan folder antrian ditolak `.gitignore`.

## Lisensi

[AGPL-3.0](LICENSE). Bridge ini memakai library `TikTokLive`; lisensi library
itu mensyaratkan AGPL untuk pemakaian pada layanan yang di-hosting.
