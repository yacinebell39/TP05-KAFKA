# TP05 - Real-Time Data Streaming and Processing Using Apache Kafka

University of Eloued - Master 2 IA & Data Science - Big Data Analytics - 2026/2027.

## حالة العمل

تم تنفيذ وتشغيل جميع التجارب بنجاح 100%، وتم التقاط جميع لقطات الشاشة في مجلد `screenshots/`، وتوثيق الإجابات والنتائج بالكامل في التقرير الشامل `REPORT.md` و`screenshots/README.md`.
الوسيط يعمل بنجاح في وضع KRaft عبر Docker Compose، والبرنامجان `producer.py` و `consumer.py` يدعمان كافة الخيارات المتقدمة.

## الملفات وتغطية المهام

| المطلوب | الملف أو الخيار |
|---|---|
| Task 1: Kafka واحد في KRaft مع Docker Compose | `compose.yaml` |
| Task 2: مكتبة Python بالإصدار المحدد | `requirements.txt` |
| Task 3: إرسال 30 حدثًا، حدث كل ثانية | `producer.py` بالإعدادات الافتراضية |
| Task 4: استقبال JSON والتنبيه عند حرارة >= 35 C | `consumer.py` بالإعدادات الافتراضية |
| Task 5: إنشاء Topic وفحص الأقسام والمجموعات | أوامر إدارة Kafka أدناه |
| إرسال 100 حدث | `producer.py --count 100` |
| المتوسط التراكمي | `consumer.py --running-average` |
| المجموعات المختلفة والمجموعة المشتركة | `consumer.py --group-id ...` |
| قسم واحد ثم ثلاثة | أوامر `--create` و`--alter` أدناه |
| إضافة المبنى والتصفية | `producer.py --include-building` و`consumer.py --building building-A` |
| CSV الاختياري | `consumer.py --csv data/events.csv` |
| أسئلة التقرير التسعة | قالب `REPORT.md`، يُستكمل في المرحلة القادمة |
| الصور | قائمة الأدلة في `screenshots/README.md` |

## نموذج البيانات

- `event_id`: رقم من 1 إلى عدد الأحداث في كل تشغيل؛ يبدأ من جديد عند إعادة تشغيل المنتج.
- `sensor_id`: من `sensor-01` إلى `sensor-04`.
- `temperature_c`: قراءة اصطناعية بين 18 و42، مقربة إلى منزلتين.
- `humidity_pct`: قراءة اصطناعية بين 25 و85، مقربة إلى منزلتين.
- `timestamp`: وقت توليد الحدث بصيغة ISO 8601 وUTC.
- `campus_building`: حقل إضافي عند تفعيل `--include-building` فقط؛ كل حساس مرتبط بمبنى ثابت من A إلى D.

المنتج يستعمل `acks="all"` وينتظر إقرار الوسيط قبل طباعة `Sent`، ويطبع القسم وOffset الفعليين.
أضيف `KAFKA_LOG_DIRS` إلى إعداد الورقة لربط سجل البيانات صراحةً بمسار الـ Volume.
برنامجا Python يعملان على Windows ويتصلان بـ `localhost:9092`؛ إعداد المستمع الحالي مخصص لذلك.

## إعداد البيئة لاحقًا - Task 2

الورقة تشترط Python 3.10 أو أحدث و`kafka-python==2.0.2`.
Python المكتشف على الجهاز هو 3.14، ولم نتحقق من توافقه مع هذه المكتبة القديمة.
الخطة هي تجهيز Python 3.11 مستقل للمشروع قبل تنفيذ الأوامر التالية، أو إثبات توافق البيئة البديلة أولًا.
لم يظهر Python 3.11 في الفحص الحالي. ثبّته أولًا من المصدر الرسمي. الأوامر التالية تفترض مسار التثبيت الافتراضي للمستخدم، وتستعمله مباشرةً دون الحاجة إلى الأمر `py`:

```powershell
cd D:\University\tp05-kafka
& "$env:LOCALAPPDATA\Programs\Python\Python311\python.exe" -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
```

نستعمل مفسر البيئة مباشرة، فلا يلزم تغيير PowerShell Execution Policy لتفعيلها.
إذا فتحت نافذة أخرى، انتقل إلى المجلد نفسه واستعمل مفسر `.venv` نفسه.

## تشغيل Kafka وإدارة Topic لاحقًا - Tasks 1 و5

```powershell
docker compose up -d
docker compose ps
docker compose logs --tail=50 kafka
```

انتظر حتى يصبح الوسيط جاهزًا في السجلات، ثم أنشئ Topic قبل تشغيل البرامج:

```powershell
docker exec tp05-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic campus-sensors --partitions 3 --replication-factor 1
docker exec tp05-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic campus-sensors
```

عرض المجموعات بعد بدء المستهلك، وفحص Offsets وLag بعد استقبال الرسائل:

```powershell
docker exec tp05-kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --list
docker exec tp05-kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group tp05-campus-monitor
```

إذا غيّرت منفذ المضيف، غيّر عنوان `KAFKA_ADVERTISED_LISTENERS` أيضًا، ومرر `--bootstrap-server localhost:NEW_PORT` إلى البرنامجين.

## التشغيل الأساسي لاحقًا - Tasks 3 و4

في Terminal 2، شغّل المستهلك أولًا:

```powershell
.\.venv\Scripts\python.exe consumer.py
```

في Terminal 1:

```powershell
.\.venv\Scripts\python.exe producer.py
```

اجمع صور المخرجات، ثم أوقف المستهلك بـ Ctrl+C حتى يغلق الاتصال بطريقة طبيعية.
الإعداد الافتراضي هو المجموعة `tp05-campus-monitor` مع `earliest` والحفظ التلقائي للـ Offsets.
`earliest` يُطبّق عند غياب Offset محفوظ صالح، ولا يعيد قراءة كل الرسائل تلقائيًا عند كل تشغيل.

## التجربة 1: إرسال 100 حدث والتجربة 2: المتوسط التراكمي

استخدم Topic مستقلًا لعزل هذه القراءات عن الثلاثين الأساسية:

```powershell
docker exec tp05-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic campus-sensors-100 --partitions 3 --replication-factor 1
```

ابدأ المستهلك في نافذة، ثم المنتج في نافذة أخرى:

```powershell
.\.venv\Scripts\python.exe consumer.py --topic campus-sensors-100 --group-id tp05-average-100 --running-average --max-messages 100
.\.venv\Scripts\python.exe producer.py --topic campus-sensors-100 --count 100 --seed 5
```

القيمة المعروضة هي مجموع درجات الحرارة مقسومًا على عدد الأحداث التي استلمها هذا المستهلك منذ بدء تشغيله.
استخدم `--seed 5` أيضًا في تشغيل مقارنة من 30 حدثًا على Topic مستقل إذا أردت مقارنة قراءات قابلة للتكرار؛ التوقيت وOffsets لا يثبتهما Seed.
وثّق العدد والمدة المقاسة والتنبيهات والمتوسط، ولا تفترض النتائج مسبقًا.

## التجربة 3: Consumer Groups

### مجموعتان مختلفتان

```powershell
docker exec tp05-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic campus-sensors-independent --partitions 3 --replication-factor 1
```

شغّل كل مستهلك في نافذة منفصلة، ثم شغّل المنتج في نافذة ثالثة:

```powershell
.\.venv\Scripts\python.exe consumer.py --topic campus-sensors-independent --group-id tp05-independent-A
.\.venv\Scripts\python.exe consumer.py --topic campus-sensors-independent --group-id tp05-independent-B
.\.venv\Scripts\python.exe producer.py --topic campus-sensors-independent --count 100 --seed 5
```

### مستهلكان في المجموعة نفسها

```powershell
docker exec tp05-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic campus-sensors-shared --partitions 3 --replication-factor 1
```

شغّل الأمر الأول مرتين، في نافذتين منفصلتين، قبل تشغيل المنتج:

```powershell
.\.venv\Scripts\python.exe consumer.py --topic campus-sensors-shared --group-id tp05-shared
.\.venv\Scripts\python.exe producer.py --topic campus-sensors-shared --count 100 --seed 5
docker exec tp05-kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group tp05-shared
```

قارن الأقسام وOffsets والأعداد في النافذتين. لا تفترض تقسيم الرسائل إلى نصفين متساويين.
المتوسط المحسوب محلي لكل مستهلك؛ المتوسط العام للمجموعة يحتاج تجميع نتائج أعضائها.

## التجربة 4: قسم واحد ثم ثلاثة أقسام

إنشاء Topic تجريبي بقسم واحد:

```powershell
docker exec tp05-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic campus-sensors-partitions --partitions 1 --replication-factor 1
docker exec tp05-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic campus-sensors-partitions
```

شغّل المستهلك التالي مرتين، في نافذتين، ثم شغّل المنتج:

```powershell
.\.venv\Scripts\python.exe consumer.py --topic campus-sensors-partitions --group-id tp05-partition-comparison
.\.venv\Scripts\python.exe producer.py --topic campus-sensors-partitions --count 100 --seed 5
```

بعد اكتمال الدفعة الأولى وتوثيقها، اترك المستهلكين يعملان ثم زد عدد الأقسام:

```powershell
docker exec tp05-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --alter --topic campus-sensors-partitions --partitions 3
docker exec tp05-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic campus-sensors-partitions
.\.venv\Scripts\python.exe producer.py --topic campus-sensors-partitions --count 100 --seed 5
docker exec tp05-kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group tp05-partition-comparison
```

وثّق توزيع الأقسام قبل التغيير وبعده. الدفعة الثانية تبدأ `event_id` من 1 مجددًا؛ هو ليس معرّفًا عالميًا فريدًا.
Kafka لا يقلل عدد الأقسام مباشرة؛ لذلك لا نحاول تغيير Topic الأساسي من 3 إلى 1.
زيادة الأقسام لا تعيد توزيع الرسائل القديمة. يبقى Broker واحد وReplication Factor يساوي 1.

## التجربة 5: المباني والتصفية

```powershell
docker exec tp05-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic campus-sensors-buildings --partitions 3 --replication-factor 1
```

ابدأ المستهلك، ثم المنتج في نافذة أخرى:

```powershell
.\.venv\Scripts\python.exe consumer.py --topic campus-sensors-buildings --group-id tp05-building-A --building building-A --running-average --max-messages 100
.\.venv\Scripts\python.exe producer.py --topic campus-sensors-buildings --count 100 --include-building --seed 5
```

التصفية تتم داخل تطبيق المستهلك بعد الاستلام. تُعرض تنبيهات المبنى المحدد فقط.
المتوسط، إذا فُعّل، يشمل **كل الأحداث المستلمة قبل التصفية**. يعرض الملخص العدد المستلم والعدد المعروض منفصلين.
الأحداث التي لا تحتوي `campus_building` لا تطابق فلتر المبنى، ويُطبع تحذير عند ظهورها.

## التجربة الاختيارية: CSV

بعد إنتاج أحداث تجربة المباني، يمكن لمجموعة مستقلة قراءة الأحداث المحفوظة وتصديرها:

```powershell
.\.venv\Scripts\python.exe consumer.py --topic campus-sensors-buildings --group-id tp05-csv-export --csv data/events.csv --max-messages 100
```

الأعمدة بالضبط: `event_id,sensor_id,temperature_c,humidity_pct,timestamp`.
عند الجمع بين `--building` و`--csv` تُحفظ الأحداث المطابقة فقط.
يرفض البرنامج الكتابة فوق ملف CSV موجود؛ استخدم اسمًا جديدًا عند إعادة التجربة.
مجلد `data/` مستبعد من Git افتراضيًا؛ أرفق عينة محددة لاحقًا فقط إذا احتجتها كدليل.

## ملاحظات لتنفيذ التجارب بصورة صحيحة

- الأوامر المتتالية لمنتج ومستهلك في الأمثلة تعني تشغيلهما في نافذتين، وليست سلسلة تُنفَّذ في نافذة واحدة.
- `--max-messages N` يعد جميع الأحداث المستلمة، بما فيها المستبعدة بفلتر المبنى، وينتظر إذا لم يصل العدد المطلوب. Ctrl+C متاح للإيقاف.
- أسماء Topics والمجموعات في الأمثلة تفترض أول تشغيل. لإعادة تجربة معزولة استخدم اسم Topic جديدًا في الإنشاء والبرنامجين؛ `--if-not-exists` لا يفرغ Topic موجودًا.
- الرسائل تبقى بعد قراءتها. المجموعة الجديدة مع `earliest` تقرأ الرسائل المحتفظ بها من البداية.
- الترتيب مضمون داخل القسم الواحد، ولا يُفترض ترتيب عام بين الأقسام.
- Auto-commit ليس ضمانًا للمعالجة مرة واحدة فقط. توقيت Commit وإعادة التشغيل قد يؤديان إلى إعادة معالجة رسائل.
- احتفظ بالأعداد والصور الفعلية عند التنفيذ، ولا تملأ التقرير بنتائج متوقعة على أنها ملاحظات.

## التسليم والتنظيف لاحقًا

أكمل أسئلة `REPORT.md` التسعة وأضف نتائج التجارب والمشكلات وحلولها، ثم التقط الصور المطلوبة.
يحتوي README النهائي على تعليمات التشغيل والإجابات، أو روابط واضحة لأقسام الإجابات في التقرير المرفق.
ارفع ملفات المشروع والتقرير والصور إلى GitHub، دون `.venv` أو بيانات الدخول أو الملفات المؤقتة.

للإيقاف مع الاحتفاظ بالبيانات:

```powershell
docker compose down
```

الأمر التالي يحذف بيانات Kafka أيضًا، ويُستخدم فقط بعد انتهاء الحاجة إلى الرسائل والأدلة:

```powershell
docker compose down -v
```

## المصادر

- ورقة TP05 المرفقة هي مرجع المتطلبات والإصدارات.
- [Apache Kafka 3.9: Basic Kafka Operations](https://kafka.apache.org/39/operations/basic-kafka-operations/)
- [kafka-python 2.0.2](https://pypi.org/project/kafka-python/2.0.2/)
- [Docker Compose](https://docs.docker.com/compose/)
