# Scalable and Cloud Programming - Earthquake Cooccurence

**Federica Santisi** — matricola 0001186853
Alma Mater Studiorum Università di Bologna

Progetto Scala/Spark per il corso di Scalable and Cloud Programming. A partire da un dataset di eventi
sismici (data, latitudine, longitudine), il job individua la coppia di celle geografiche che è "co-occorsa" nel maggior numero di date distinte, cioè ha registrato un evento in entrambe le celle nello stesso giorno più volte di ogni altra coppia.

## Key Finding

Sul dataset completo (eventi sismici California/Nevada, ~2.9M righe), la coppia di celle vincente è:

```
((38.8, -122.8), (38.8, -122.7))
```

con **10.032 co-occorrenze** (giorni distinti in cui entrambe le celle hanno registrato un evento).

## Repository Structure

```
src/main/scala/
├── Earthquake.scala          # entry point: parsing argomenti, orchestrazione, scrittura output
├── Normalizer.scala          # parsing riga CSV -> (cellCode: Long, date: String)
└── CooccurrenceFinder.scala  # dedup, conteggio coppie, recupero date della coppia vincente
history/                      # versioni precedenti del codice, vedi history/README.md
build.sbt                     # config sbt + plugin assembly
```

Il dataset **non è incluso** nel repository (vedi sezione [Dataset](#dataset)).

## Technical Requirements

- JDK 11+
- sbt 1.x
- Scala 2.12.18
- Apache Spark 3.3.2
- Google Cloud SDK (`gcloud`, `gsutil`) — solo per l'esecuzione su Dataproc

## Building

```bash
sbt assembly
```

Genera il fat jar in `target/scala-2.12/earthquake-assembly.jar`.

## Runtime Parameters

| Argomento        | Obbligatorio | Descrizione                                      |
|------------------|:------------:|---------------------------------------------------|
| `inputPath`      | sì           | percorso del CSV di input (locale o `gs://...`)    |
| `outputPath`     | sì           | percorso del file di output (locale o `gs://...`)  |
| `numPartitions`  | no           | grado di parallelismo (default: `16`)              |

## Execution Modes

**Locale** (via sbt):

```bash
sbt "run <inputPath> <outputPath> [numPartitions]"
```

oppure, dopo `sbt assembly`, con `spark-submit`:

```bash
spark-submit --master "local[*]" --class Earthquake \
  target/scala-2.12/earthquake-assembly.jar \
  <inputPath> <outputPath> [numPartitions]
```

**Google Cloud Dataproc**:

```bash
# upload jar e dataset su Cloud Storage
gsutil cp target/scala-2.12/earthquake-assembly.jar gs://<YOUR_BUCKET>/jars/
gsutil cp <your-dataset>.csv gs://<YOUR_BUCKET>/data/

# submit del job sul cluster
gcloud dataproc jobs submit spark \
  --cluster=<YOUR_CLUSTER_NAME> \
  --region=<YOUR_REGION> \
  --class=Earthquake \
  --jars=gs://<YOUR_BUCKET>/jars/earthquake-assembly.jar \
  -- \
  gs://<YOUR_BUCKET>/data/<your-dataset>.csv \
  gs://<YOUR_BUCKET>/output/result.txt \
  <numPartitions>
```

## Dataset

Il dataset (alcuni milioni di righe) non è incluso nel repository per motivi di dimensione. Per eseguire
il job serve un CSV con header contenente almeno le colonne:

```
longitude,latitude,date
```

dove `date` è una stringa che inizia con `yyyy-MM-dd` (l'eventuale timestamp dopo lo spazio viene
ignorato). Un dataset di questo tipo (eventi sismici geolocalizzati) è reperibile ad es. da cataloghi
sismici pubblici come USGS o EMSC.

## Versioni precedenti

Le implementazioni intermedie (prima dell'introduzione delle chiavi `Long` e della fase di conteggio
separata dal recupero delle date) sono conservate in [`history/`](history/README.md) a scopo
documentativo e non fanno parte della build sbt.
