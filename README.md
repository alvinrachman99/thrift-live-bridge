# thrift-live-bridge

Jembatan antara TikTok LIVE dan antrian order di Odoo. Saat host menyematkan
(pin) komentar pemenang di siaran, bridge membaca event itu dan mengirimkannya
ke Odoo sebagai satu baris antrian. Order baru dibuat di Odoo, oleh admin.

**Status: sedang dibangun.** Belum ada kode yang bisa dijalankan.

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

## Rahasia

Repo ini tidak memuat rahasia. API key dan alamat Odoo dibaca dari environment
saat program dijalankan. File `.env` dan sejenisnya ditolak `.gitignore`.

## Lisensi

[AGPL-3.0](LICENSE). Bridge ini memakai library `TikTokLive`; lisensi library
itu mensyaratkan AGPL untuk pemakaian pada layanan yang di-hosting.
