# Storico delle versioni

Questa cartella contiene le implementazioni precedenti di `CooccurrenceFinder`, mantenute solo a scopo
documentativo per mostrare la progressione delle ottimizzazioni. Non fanno parte della build sbt principale.

- **v1** (`v1/CooccurrenceFinderv1.scala`): prima versione funzionante. Celle rappresentate come tuple
  `(Double, Double)`, raggruppamento per data con `groupByKey()` e generazione delle coppie con doppio ciclo
  su liste, senza cache né riduzione gerarchica.
- **v2** (`v2/CooccurrenceFinderv2.scala`): sostituisce `groupByKey()` con `aggregateByKey` (dedup delle celle
  già durante l'aggregazione tramite `Set`), introduce `cache()` sull'RDD normalizzato per evitare ricalcoli e
  `treeReduce` per la riduzione finale in modo gerarchico. Le chiavi restano però tuple `(Double, Double)`.
- **versione finale** (in `src/main/scala/`): sostituisce le chiavi `(Double, Double)` con un singolo `Long`
  impacchettato (`Normalizer.encodeCell`/`decodeCell`), molto più leggero da hashare/confrontare nello shuffle.
  Separa inoltre una fase di solo conteggio delle coppie da una fase successiva di recupero delle date (solo
  sulla coppia vincente, sfruttando l'RDD intermedio già persistito), e rende `numPartitions` parametrico da
  riga di comando invece di lasciarlo implicito.
