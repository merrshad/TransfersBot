# راه‌اندازی روی VPS (۲ گیگ رم / ۲ هسته)

## آیا ۲GB RAM و ۲ vCPU کافی است؟

**بله، برای این ربات کافی است.** مصرف تقریبی:

| بخش | مصرف |
|---|---|
| پایتون + ربات در حال اجرا | حدود ۱۵۰–۳۰۰ مگابایت |
| Docker daemon و سرویس‌های پایه | حدود ۱۰۰–۲۰۰ مگابایت |
| سیستم‌عامل Ubuntu minimal | حدود ۳۰۰–۵۰۰ مگابایت |
| اوج مصرف هنگام build | حدود ۷۰۰ مگابایت تا ۱٫۲ گیگابایت |

نکته‌ها:

- برای build اولیه و ایمنی، **۲ گیگابایت swap** اضافه کنید.
- `docker-compose.yml` یک `mem_limit: 1g` دارد تا یک allocation runaway کل
  VPS را از کار نیندازد.
- فقط **یک** کانتینر/worker روی یک دیتابیس SQLite اجرا کنید. horizontal
  scaling پشتیبانی نمی‌شود.
- حداقل ۲۰ گیگابایت دیسک SSD توصیه می‌شود؛ فایل‌های موقت رسانه و backupها
  فضا می‌گیرند.

---

## ۱. انتخاب لوکیشن VPS

ربات باید هم‌زمان به این دو دسترسی داشته باشد:

```bash
curl -I https://api.telegram.org
curl -I https://tapi.bale.ai
```

- **تلگرام از داخل ایران معمولاً در دسترس نیست.** بهترین گزینه یک VPS
  خارج از ایران است (آلمان، هلند، فنلاند، …). بله از خارج هم در دسترس است.
- اگر VPS داخل ایران است، برای تلگرام به proxy/VPN معتبر نیاز دارید و باید
  `HTTPS_PROXY` را تنظیم کنید؛ برای production توصیه نمی‌شود.
- پورت ورودی لازم نیست؛ ربات فقط long polling خروجی روی ۴۴۳ انجام می‌دهد.

---

## ۲. آماده‌سازی اولیه سرور

با کاربر root یا sudo:

```bash
sudo apt update && sudo apt upgrade -y
sudo apt install -y curl ca-certificates ufw fail2ban unattended-upgrades
sudo timedatectl set-timezone UTC
```

### ساخت swap (مهم برای ۲GB)

```bash
sudo fallocate -l 2G /swapfile
sudo chmod 600 /swapfile
sudo mkswap /swapfile
sudo swapon /swapfile
echo '/swapfile none swap sw 0 0' | sudo tee -a /etc/fstab
echo 'vm.swappiness=10' | sudo tee /etc/sysctl.d/99-swap.conf
sudo sysctl --system
free -h
```

### فایروال

فقط SSH را باز کنید. ربات هیچ پورت ورودی ندارد:

```bash
sudo ufw default deny incoming
sudo ufw default allow outgoing
sudo ufw allow OpenSSH
sudo ufw enable
sudo ufw status
```

### امنیت SSH

**قبل از این کار مطمئن شوید کلید SSH شما کار می‌کند**، وگرنه ممکن است از
سرور قفل شوید:

```bash
sudo sed -i 's/^#\?PasswordAuthentication .*/PasswordAuthentication no/' /etc/ssh/sshd_config
sudo sed -i 's/^#\?PermitRootLogin .*/PermitRootLogin no/' /etc/ssh/sshd_config
sudo sshd -t
sudo systemctl reload ssh
```

اگر کاربر `deploy` ندارید:

```bash
sudo adduser deploy
sudo usermod -aG sudo deploy
# کلید عمومی خود را در /home/deploy/.ssh/authorized_keys قرار دهید
```

---

## ۳. نصب Docker

روش رسمی Docker روی Ubuntu:

> هر دستور را جداگانه و در **یک خط** کپی کنید. بک‌اسلش آخر خط
> (`\`) اگر با خط خالی یا فاصله اشتباه کپی شود، دستور را می‌شکند.

```bash
sudo install -m 0755 -d /etc/apt/keyrings
sudo curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
sudo chmod a+r /etc/apt/keyrings/docker.asc
echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/ubuntu $(. /etc/os-release && echo "$VERSION_CODENAME") stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
sudo apt update
sudo apt install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
sudo systemctl enable --now docker
sudo usermod -aG docker "$USER"
```

از حساب خارج و دوباره وارد شوید تا گروه `docker` اعمال شود. بررسی:

```bash
docker --version
docker compose version
```

> عضویت در گروه `docker` عملاً معادل دسترسی root است؛ کاربر deploy را
> محافظت‌شده نگه دارید.

---

## ۴. انتقال پروژه به سرور

### روش A: از ویندوز با tar و scp

در PowerShell، داخل پوشهٔ پروژه:

```powershell
tar --exclude=.venv --exclude=data --exclude=.env --exclude=__pycache__ `
    -czf bridge.tar.gz .
scp bridge.tar.gz deploy@SERVER_IP:/tmp/
```

روی سرور:

```bash
sudo mkdir -p /opt/bale-bridge
sudo chown "$USER":"$USER" /opt/bale-bridge
tar -xzf /tmp/bridge.tar.gz -C /opt/bale-bridge
cd /opt/bale-bridge
```

### روش B: Git

اگر پروژه را در Git دارید:

```bash
sudo mkdir -p /opt/bale-bridge
sudo chown "$USER":"$USER" /opt/bale-bridge
git clone <REPOSITORY_URL> /opt/bale-bridge
cd /opt/bale-bridge
```

---

## ۵. تنظیم `.env`

```bash
cd /opt/bale-bridge
cp .env.example .env
chmod 600 .env
nano .env
```

حداقل این چهار مقدار را پر کنید:

```dotenv
TELEGRAM_BOT_TOKEN=...
BALE_BOT_TOKEN=...
TELEGRAM_OWNER_ID=123456789
BALE_OWNER_ID=123456789
```

پیشنهاد تنظیمات برای VPS ۲ گیگی و استقرار عمومی:

```dotenv
DB_PATH=/app/data/bridge.db
MEDIA_TEMP_DIR=/app/media
HEALTHCHECK_FILE=/app/data/health

MAX_USERS=500
MAX_PAIRS_PER_USER=1
PAIR_REQUEST_TTL_HOURS=24

MEDIA_ENABLED=true
MEDIA_USER_QUOTA_MB=100
MEDIA_GROUP_WINDOW_SECONDS=0.8

OUTBOX_MAX_ATTEMPTS=5
OUTBOX_RETRY_BASE_SECONDS=10
OUTBOX_RETRY_JITTER=0.2
OUTBOX_MEDIA_TIMEOUT_TERMINAL=true
OUTBOX_SENT_RETENTION_DAYS=7
SHUTDOWN_TIMEOUT_SECONDS=30

RATE_LIMIT_ENABLED=true
USER_MESSAGES_PER_MINUTE=20
USER_RATE_LIMIT_ACTION=queue
TELEGRAM_CHAT_MESSAGES_PER_SECOND=1
BALE_CHAT_MESSAGES_PER_SECOND=1

LOG_LEVEL=INFO
```

### سینک گروه به گروه

پل گروهی از همان توکن‌ها و همان کانتینر استفاده می‌کند. چند جفت‌گروه
مستقل به‌صورت هم‌زمان پشتیبانی می‌شوند. برای هر جفت گروه:

1. ربات تلگرام را عضو گروه کنید و **ادمین** کنید (یا در BotFather با
   `/setprivacy` حالت Privacy را غیرفعال کنید) تا همه پیام‌های گروه را ببیند.
2. ربات بله را عضو گروه بله کنید و در صورت نیاز ادمین کنید.
3. سینک گروهی را در `.env` روشن کنید:

```dotenv
GROUP_SYNC_ENABLED=true
GROUP_INVITE_TTL_MINUTES=15
```

`TELEGRAM_GROUP_ID` و `BALE_GROUP_ID` فقط یک لینک اولیه در اولین اجرا
می‌سازند. بعد از آن همهٔ لینک‌ها را از داخل خود گروه‌ها مدیریت کنید.

#### ساخت و قطع لینک از داخل ربات (فقط مالک)

در گروه اول (مثلاً تلگرام) دستور را بفرستید:

```text
/groupsync
```

ربات یک کد یک‌بارمصرف می‌دهد. همان کد را در گروه دوم (مثلاً بله) بفرستید:

```text
/groupsync ABC12345
```

اتصال فعال می‌شود. برای دیدن همهٔ جفت‌گروه‌ها:

```text
/groupsync list
```

برای قطع سینک همان گروه:

```text
/groupsync off
```

قوانین:

- فقط مالک ربات (شناسه‌های `TELEGRAM_OWNER_ID` و `BALE_OWNER_ID`) می‌تواند
  لینک بسازد یا قطع کند.
- هر گروه در هر لحظه فقط در یک لینک فعال می‌تواند باشد.
- کد اتصال یک‌بارمصرف و به‌طور پیش‌فرض ۱۵ دقیقه معتبر است.
- لینک‌های مختلف کاملاً مستقل‌اند؛ پیام هر جفت فقط به مقصد همان جفت می‌رود.
- پیام‌های متنی، عکس، ویدیو، فایل و آلبوم در هر دو جهت منتقل می‌شوند.
- نام فرستنده به‌صورت `👤 نام:` در ابتدای متن/کپشن اضافه می‌شود.
- پیام‌های ربات‌ها و دستورها (مثل `/start`) منتقل نمی‌شوند.

برای تست اتصال، پس از بالا آمدن سرویس یک پیام در هر گروه بفرستید و لاگ را
بررسی کنید:

```bash
docker compose logs -f bridge | grep -E "group bridge|group relay|group link|group invite"
```

نکته‌ها:

- `DB_PATH` و `MEDIA_TEMP_DIR` در Compose توسط volumeها بازنویسی می‌شوند؛
  مقادیر داخل کانتینر `/app/data` و `/app/media` هستند.
- `MEDIA_USER_QUOTA_MB=100` برای دیسک کوچک منطقی‌تر از ۲۰۰ است.
- `MAX_USERS=0` را برای استقرار عمومی نگذارید.
- `.env` را هرگز کامیت یا در چت ارسال نکنید.
- توکن‌هایی که قبلاً در چت به اشتراک گذاشته شده‌اند را در BotFather
  revoke کنید.

---

## ۶. اجرا

```bash
cd /opt/bale-bridge
docker compose build
docker compose up -d
docker compose ps
```

اولین build ممکن است چند دقیقه طول بکشد. با ۲GB RAM و swap نباید OOM شود.

بررسی health:

```bash
docker compose ps
docker compose exec bridge python healthcheck.py
docker compose logs --tail=100 -f
```

انتظار:

```text
STATUS: Up ... (healthy)
OK: healthy (age ...s)
```

---

## ۷. تست دستی اولیه

1. در بله به `@YourBaleBot` `/start` بفرستید.
2. در تلگرام به ربات خودتان `/start` بفرستید.
3. `/whoami` بزنید؛ باید شناسهٔ کوتاه تصادفی (مثل `A7K9Q2M4`) ببینید.
4. اگر جفت فعال ندارید: یک طرف `/pair <public_id طرف مقابل>` و طرف دیگر
   دکمهٔ «✅ پذیرش» یا `/accept <request_id>`.
5. یک متن و یک عکس در هر دو جهت بفرستید.
6. `/status` مالک را ببینید.
7. یک فایل متوسط بفرستید و مطمئن شوید فقط یک نسخه می‌رسد.

---

## ۸. backup دیتابیس

SQLite در حالت WAL است؛ برای backup سازگار از خود SQLite استفاده کنید.
روی سرور:

```bash
mkdir -p ~/backups

docker compose exec -T bridge python -c "
import sqlite3
src = sqlite3.connect('/app/data/bridge.db')
dst = sqlite3.connect('/tmp/bridge-backup.db')
src.backup(dst)
dst.close()
src.close()
"
docker cp bale-telegram-bridge:/tmp/bridge-backup.db \
  ~/backups/bridge-$(date +%F-%H%M).db
docker compose exec -T bridge rm -f /tmp/bridge-backup.db
```

برای cron روزانه:

```bash
crontab -e
```

```cron
15 3 * * * cd /opt/bale-bridge && docker compose exec -T bridge python -c "import sqlite3; s=sqlite3.connect('/app/data/bridge.db'); d=sqlite3.connect('/tmp/bridge-backup.db'); s.backup(d); d.close(); s.close()" && docker cp bale-telegram-bridge:/tmp/bridge-backup.db /home/deploy/backups/bridge-$(date +\%F).db && docker compose exec -T bridge rm -f /tmp/bridge-backup.db
```

فقط ۷ نسخهٔ آخر را نگه دارید:

```bash
find ~/backups -name 'bridge-*.db' -mtime +7 -delete
```

---

## ۹. به‌روزرسانی

قبل از هر update یک backup بگیرید، سپس:

```bash
cd /opt/bale-bridge
git pull                # یا انتقال فایل‌های جدید
docker compose up -d --build
docker compose ps
docker compose logs --tail=50
```

migrationهای دیتابیس (مثل v4) خودکار در startup اجرا می‌شوند.

---

## ۱۰. نگهداری و پایش

```bash
# مصرف منابع
docker stats --no-stream

# فضای دیسک
df -h
docker system df

# لاگ‌ها (محدودشده در Compose: 10MB × 3)
docker compose logs --tail=200

# پاک‌سازی imageهای قدیمی
docker image prune -f
```

اگر فضای دیسک پر شد:

- `MEDIA_USER_QUOTA_MB` را کم کنید.
- `OUTBOX_SENT_RETENTION_DAYS` را کم کنید.
- فایل‌های کهنهٔ `/app/media` در startup پاک می‌شوند، اما quota را بررسی
  کنید.
- `docker system prune` (با احتیاط) اجرا کنید.

---

## ۱۱. عیب‌یابی سریع

| مشکل | راه‌حل |
|---|---|
| health قرمز | `docker compose logs --tail=200` و `docker compose exec bridge python healthcheck.py` |
| `ValidationError` | یکی از چهار متغیر الزامی `.env` خالی/نامعتبر است |
| `database is locked` | بیش از یک کانتینر روی یک دیتابیس اجرا نکنید |
| تلگرام وصل نمی‌شود | `curl -I https://api.telegram.org`؛ VPS خارج از ایران یا proxy |
| build با OOM قطع می‌شود | swap را چک کنید (`free -h`) و دوباره build کنید |
| `no space left on device` | `df -h`, `docker system df`, quota و retention را کم کنید |
| permission denied روی volume | از named volumeهای Compose استفاده کنید؛ کاربر کانتینر UID 10001 است |
| فایل تکراری ارسال می‌شود | `OUTBOX_MEDIA_TIMEOUT_TERMINAL=true` و timeoutهای Telegram را چک کنید (پیش‌فرض درست است) |

---

## ۱۲. چک‌لیست نهایی

- [ ] VPS خارج از ایران است یا proxy معتبر دارد.
- [ ] `curl -I https://api.telegram.org` و `curl -I https://tapi.bale.ai`
      موفق‌اند.
- [ ] swap فعال است (`free -h`).
- [ ] UFW فقط SSH را باز کرده است.
- [ ] ورود با پسورد/root غیرفعال و کلید SSH فعال است.
- [ ] Docker و Compose نصب‌اند.
- [ ] `.env` با `chmod 600` محافظت شده و توکن‌ها rotate شده‌اند.
- [ ] `docker compose up -d` و health سبز است.
- [ ] backup روزانه تنظیم شده است.
- [ ] `MAX_USERS` و rate limitها برای استقرار عمومی تنظیم شده‌اند.
