# SCP_DataMiningProject_FP-Growth
## Progetto Scalable and Cloud Programming. Magistrale Informatica Bologna
### Introduzione:
   * Questo progetto è stato sviluppato per il corso **Scalable and Cloud Programming** (Prof. **_Gianluigi Zavattaro_**) della laurea magistrale di Informatica a Bologna.  
   Ci siamo occupati del problema del **Frequent Itemset Mining** cioè del metodo che si occupa di trovare **regole associative** tra item. Il classico **esempio** a cui viene applicato è il **market basket analysis**, cioè fare un analisi sulle **abitudini di acquisto** dei clienti in un supermercato, con lo scopo di poter **aumentare i guadagni**.  
  Come obiettivo del corso avevamo quello di imparare l'approccio funzionale alla realizzazione di sistemi scalabili tramite linguaggi e framework quali **Scala** e **Spark**. Abbiamo di conseguenza sviluppato il nostro codice in locale, usufruendo dell'IDE **IntelliJ** e del linguaggio scala abbinato a **Spark**. Infine ci è stato richiesto di portare il nostro codice su un sistema **realmente distribuito**, in particolare sulla **Google Cloud Platform**. Per questo motivo mostriamo come poter configurare quest'ultima in modo tale da poter testare il nostro codice.



### Configurazione Google Cloud Platform:
 <!--  <details><summary> 0. Pre-requisiti:  </summary>
<p>
    Prima di iniziare la configurazione è necessario **creare un progetto** rinominato come _"ProgettoSCP"_ sul proprio profilo di **Google Cloud Platform**.      Assicurarsi inoltre di avere del **credito** per poter testare il tutto.
</p>
</details> -->

  #### 0. Pre-requisiti
  Prima di iniziare la configurazione è necessario **creare un progetto** rinominato come _"ProgettoSCP"_ sul proprio profilo di **Google Cloud Platform**.      Assicurarsi inoltre di avere del **credito** per poter testare il tutto.

  #### 1. Creazione Bucket
  Per prima cosa bisogna creare il **bucket** nella quale andremo a mettere i dataset ed il jar che vogliamo eseguire. Successivamente verrà usato anche per mettere i risultati ottenuti. Questo passaggio, come tutti i successivi, potrà essere svolto sia tramite **interfaccia** della GCP sia tramite **comandi da terminale**.
  Questo bucket sarà chiamato _bucket-dataset-scp_ e......

  Codice da eseguire sul terminale per avere il bucket uguale a quello creato da noi:
  ```
  gsuitl://creazioneBucket
  ```

  #### 2. Creazione Cluster
  Dopo aver creato il bucket è necessario creare il **cluster** che sarà formato dalle macchine che eseguiranno il nostro codice. Rechiamoci dunque nella sezione _Dataproc_ e creiamolo. Nel nostro caso avevamo poco credito a disposizione, di conseguenza abbiamo optato per creare un cluster composto da **1 Master ed N Workers** (inizialmente 2 Workers).

  ```
  gsuitl://creazioneCluster
  ```

  #### 3. Avviare il cluster
  Per avviare il cluster possiamo semplicemente selezionare _cluster_1_ dalla lista dei nostri cluster e cliccare su _Avvia_


  #### 4. Creare il .jar del nostro codice

  #### 5. Creare un job con il nostro jar

  #### 6. Visualizzare il risultato
  Se l'algoritmo che abbiamo eseguito è **FPGrowthRDD** allora ci basterà scrivere sul terminale:
  ```
  gsutil cat gs://bucket-dataset-scp/output/FPGrowthRDDResult/*
  ```
 

