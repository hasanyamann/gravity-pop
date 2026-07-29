# Ev Tipi EV Şarj İzleme ve Uzaktan Kontrol

Vestel ev tipi şarj cihazınızı telefondan izlemek ve kontrol etmek için
kendi sunucunuz. Şarj sırasında **anlık kW**, **verilen kWh**, **geçen
süre** ve **tahmini ücret** canlı olarak görünür; şarjı telefondan
**başlatıp durdurabilirsiniz**.

Gerçek cihaza dokunmadan denemek için birlikte gelen **simülatör** aynı
protokolü konuşan sanal bir şarj cihazıdır.

---

## Önce şunu netleştirelim: firmware güncellemiyoruz

Vestel'in cihaz yazılımı kapalıdır; içine kendi kodumuzu koyamayız ve
buna gerek de yok. Cihazda zaten istediğimiz her şeyi yapan bir arayüz
var: **OCPP 1.6J**. Bu, şarj cihazlarının bir "merkezi sistem" ile
konuşmak için kullandığı standart protokol. Vestel EVC serisi bunu
destekler ve normalde üreticinin bulut sunucusuna bağlanır.

Yaptığımız şey, cihazın ayarlarından **merkezi sistem adresini kendi
sunucumuza çevirmek**. Cihaz bize bağlanır, ölçümlerini bize gönderir,
komutlarımızı dinler. Firmware'e hiç dokunulmaz, geri alınabilir bir
ayar değişikliğidir.

> **Önemli:** OCPP'de cihaz aynı anda tek bir merkezi sisteme bağlanır.
> Adresi kendi sunucunuza çevirdiğinizde Vestel'in kendi mobil
> uygulaması bu cihaz için çalışmayı bırakır. Geri dönmek isterseniz
> eski adresi geri yazmanız yeterli — bu yüzden değiştirmeden önce
> mevcut ayarı **not alın**.

İstediğiniz üç bilgi de protokolün doğal çıktısıdır:

| İstenen           | Nereden geliyor                                              |
| ----------------- | ------------------------------------------------------------ |
| Anlık kW          | `MeterValues` → `Power.Active.Import`                         |
| Verilen kWh       | `Energy.Active.Import.Register` eksi `StartTransaction`'daki `meterStart` |
| Geçen süre        | Şarjın başladığı an ile şimdi arasındaki fark                 |
| Uzaktan başlat    | `RemoteStartTransaction`                                      |
| Uzaktan durdur    | `RemoteStopTransaction`                                       |

---

## Yapı

```
   Şarj cihazı                    Sunucu (Raspberry Pi / PC)            Telefon
   (Vestel ya da simülatör)
                                 ┌───────────────────────────┐
   OCPP 1.6J  ──────────────────▶│  :9000  OCPP sunucusu     │
   WebSocket  ◀──────────────────│         (central_system)   │
                                 │                            │
                                 │  :8080  HTTP + WebSocket   │◀── tarayıcı / PWA
                                 └───────────────────────────┘
```

| Dosya               | Görevi                                                        |
| ------------------- | ------------------------------------------------------------- |
| `ocpp_core.py`      | OCPP 1.6J mesaj katmanı (çerçeveleme, istek/yanıt eşleştirme)  |
| `central_system.py` | Merkezi sistem, oturum takibi, telefon API'si                  |
| `simulator.py`      | Sanal Vestel cihazı — gerçekçi şarj eğrisiyle                  |
| `web/index.html`    | Telefon arayüzü (ana ekrana eklenebilen PWA)                   |
| `test_e2e.py`       | Uçtan uca testler                                              |

---

## Kurulum

```bash
cd evcharger
pip3 install -r requirements.txt
```

Python 3.10 veya üstü gerekir.

## Simülasyonla deneme

En kolayı tek komut:

```bash
./run_demo.sh
```

Sonra tarayıcıdan **http://localhost:8080** adresini açın. "Şarjı
Başlat" düğmesine basın; güç grafiğinin yükseldiğini, kWh ve sürenin
arttığını göreceksiniz.

Ayrı ayrı çalıştırmak isterseniz iki terminal açın:

```bash
# 1. terminal — sunucu
python3 central_system.py --price 3.20

# 2. terminal — sanal cihaz
python3 simulator.py
```

Simülatörle deneyebileceğiniz senaryolar:

```bash
python3 simulator.py --phases 3 --max-current 32      # 22 kW üç faz
python3 simulator.py --battery 40 --soc 88            # neredeyse dolu batarya (akım kısılması)
python3 simulator.py --battery 3                      # küçük batarya: şarj birkaç dakikada biter
python3 simulator.py --unplugged                      # kablo takılı değil
python3 simulator.py --auto-start 10                  # 10 sn sonra kendiliğinden başlar (RFID gibi)
```

### `--speed` hakkında bir uyarı

Simülatörün `--speed` seçeneği zamanı hızlandırır (`--speed 60` ile 1
gerçek saniye 1 simülasyon dakikası olur) ve testleri kısaltmak için
vardır. Ama sunucu süreyi **gerçek saat** ile ölçtüğü hâlde enerji
hızlandırılmış zamanla biriktiği için, hızlandırılmış çalışmada
"Ortalama kW" ve "Süre" alanları anlamsız görünür — örneğin 7 kW'lık
şarjda ortalama 400 kW yazar. Anlık kW, kWh ve düğmeler doğru çalışır.

Tutarlı sayılarla hızlı bir demo istiyorsanız zamanı hızlandırmak
yerine bataryayı küçültün: `--battery 3`. Gerçek cihazda böyle bir
sorun yoktur.

## Testler

```bash
python3 test_e2e.py
```

Şarjın başlaması, ölçümlerin akması, kWh/sürenin artması, uzaktan
durdurma, batarya dolunca kendiliğinden bitme, wifi koptuğunda oturumun
korunması ve API belirteci kontrolü test edilir.

---

## Gerçek Vestel cihazına bağlama

**1. Sunucuyu evde sürekli açık bir makinede çalıştırın.** Raspberry Pi
idealdir. Makineye sabit bir yerel IP verin (modem arayüzünden DHCP
rezervasyonu) — cihaz bu adrese bağlanacak.

```bash
python3 central_system.py --price 3.20 --token "uzun-bir-parola"
```

**2. Cihazın yapılandırma arayüzüne girin.** Vestel EVC serisinde bu,
cihazın yerel IP adresinden ulaşılan bir web arayüzüdür (bazı
modellerde cihazın kendi wifi erişim noktası üzerinden). Kesin adres ve
giriş bilgisi modelinize ve firmware sürümünüze göre değişir; cihazın
kılavuzuna ya da Vestel destek hattına bakın.

**3. OCPP ayarlarını bulun ve şunları yazın:**

| Ayar                                    | Değer                        |
| --------------------------------------- | ---------------------------- |
| OCPP sürümü                             | `1.6` (JSON / SOAP değil)    |
| Merkezi sistem adresi (Central System URL) | `ws://192.168.1.50:9000`  |
| Şarj cihazı adı (ChargePoint / ChargeBoxId) | `VESTEL-EV-01`           |

`192.168.1.50` yerine sunucunun IP'sini yazın. Ayarların menüdeki
isimleri firmware'e göre değişebilir ("OCPP Configuration", "Backend",
"Central System" gibi başlıklar altında olur).

**4. Cihazı yeniden başlatın.** Sunucu kaydında şuna benzer satırlar
görmelisiniz:

```
VESTEL-EV-01 baglandi (/VESTEL-EV-01)
VESTEL-EV-01 acildi: Vestel EVC04-AC22 (fw ...)
VESTEL-EV-01 konnektor 1 -> Preparing (NoError)
```

**5. Telefondan açın.** Tarayıcıda `http://192.168.1.50:8080/?token=uzun-bir-parola`
adresine gidin. Belirteç bir kez girilir ve saklanır. Paylaş menüsünden
"Ana Ekrana Ekle" derseniz uygulama gibi açılır.

### Cihaz bağlanmıyorsa

- Sunucunun 9000 portu güvenlik duvarında açık mı?
  (`sudo ufw allow 9000/tcp`)
- Cihaz ile sunucu aynı ağda mı? Şarj cihazları genelde misafir ağından
  yerel ağa erişemez.
- Adresi `ws://` ile yazdınız mı? (`http://` değil)
- Bazı firmware'ler adresin sonuna cihaz adını kendisi ekler, bazıları
  eklemenizi bekler. Sunucu her iki durumu da kabul eder; kayıtta hangi
  adresle bağlandığını görebilirsiniz.
- Ayrıntılı kayıt için sunucuyu `-v` ile çalıştırın, tüm OCPP
  mesajlarını görürsünüz.

---

## Telefon API'si

Belirteç tanımlıysa her istekte `X-Auth-Token` başlığı ya da `?token=`
parametresi gerekir.

| Yöntem | Adres          | Açıklama                                        |
| ------ | -------------- | ----------------------------------------------- |
| `GET`  | `/api/state`   | Anlık durumun tamamı                            |
| `GET`  | `/api/ws`      | WebSocket — durum değiştikçe canlı akış         |
| `POST` | `/api/start`   | Şarjı başlat                                    |
| `POST` | `/api/stop`    | Şarjı durdur                                    |
| `POST` | `/api/refresh` | Cihazdan hemen yeni ölçüm iste                  |
| `GET`  | `/api/history` | Geçmiş şarj oturumları                          |

```bash
curl -X POST http://localhost:8080/api/start -H "X-Auth-Token: parola"
curl -s http://localhost:8080/api/state | python3 -m json.tool
```

## Sunucu seçenekleri

```
--price 3.20            kWh birim fiyatı (ücret hesabı için)
--currency TL           para birimi etiketi
--token GIZLI           telefon API'si için parola
--allow-tag ABC123      yalnızca bu RFID etiketlerini kabul et (tekrarlanabilir)
--ocpp-port 9000        şarj cihazının bağlanacağı port
--http-port 8080        telefon arayüzü portu
--data-dir ./data       geçmiş kayıtlarının tutulduğu klasör
-v                      tüm OCPP mesajlarını yaz
```

---

## Güvenlik

Bu sunucu ev ağı içinde çalışmak üzere tasarlandı. Portları doğrudan
internete açmayın: OCPP bağlantısında kimlik doğrulaması yoktur, yani
9000 portuna ulaşabilen herkes sahte bir şarj cihazı gibi davranabilir
ve 8080'e ulaşan herkes şarjınızı durdurabilir.

Dışarıdan erişmek istiyorsanız doğru yol bir VPN'dir (WireGuard ya da
Tailscale). Telefonunuz VPN üzerinden ev ağına girer, her şey yerel
kalır. `--token` mutlaka kullanın; VPN'i o da tamamlar ama yerini
tutmaz.

## Bilinmesi gerekenler

- Şarj sırasında sunucu kapanırsa cihaz şarja devam eder — durdurmak
  fiziksel olarak cihazın işidir, sunucunun değil. Sunucu geri
  açıldığında cihaz yeniden bağlanır, ama o oturumun başlangıç sayacı
  kaybolduğu için o şarjın kWh'i eksik görünebilir. Tamamlanan
  oturumlar `data/state.json` içinde saklanır.
- Wifi koparsa oturum silinmez; cihaz geri geldiğinde aynı işlem devam
  eder.
- Ücret hesabı `--price` ile verdiğiniz sabit tarifeye dayanır; çok
  zamanlı sayacınız varsa gerçek fatura farklı olacaktır.
- Doluluk oranı (%) yalnızca aracınız bunu şarj cihazına bildiriyorsa
  görünür; her araç bildirmez.
