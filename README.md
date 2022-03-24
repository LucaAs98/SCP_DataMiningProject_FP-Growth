# SCP_DataMiningProject_FP-Growth
## Progetto Scalable and Cloud Programming. Magistrale Informatica Bologna
### Introduzione:
Questo progetto è stato sviluppato per il corso **Scalable and Cloud Programming** (Prof. **_Gianluigi Zavattaro_**) della laurea magistrale di Informatica a Bologna.  
   Ci siamo occupati del problema del **Frequent Itemset Mining** cioè del metodo che si occupa di trovare **regole associative** tra item. Il classico **esempio** a cui viene applicato è il **market basket analysis**, cioè fare un analisi sulle **abitudini di acquisto** dei clienti in un supermercato, con lo scopo di poter **aumentare i guadagni**.  
  Come obiettivo del corso avevamo quello di imparare l'approccio funzionale alla realizzazione di sistemi scalabili tramite linguaggi e framework quali **Scala** e **Spark**. Abbiamo di conseguenza sviluppato il nostro codice in locale, usufruendo dell'IDE **IntelliJ** e del linguaggio **Scala** abbinato a **Spark**. Infine ci è stato richiesto di portare il nostro codice su un sistema **realmente distribuito**, in particolare sulla **Google Cloud Platform**. Per questo motivo alla fine del README, nella sezione [Configurazione Google Cloud Platform](#configurazione-google-cloud-platform), mostriamo come poter configurare quest'ultima in modo tale da poter testare il nostro codice.
  
  
### Struttura del progetto:
Innanzitutto abbiamo un package `resources` all'interno della quale sono presenti altre due cartelle:  
* `dataset` dove possiamo trovare tutti i dataset con la quale poter testare gli algoritmi;
* `results` nella quale poter salvare i risultati di output.

Il fulcro del nostro progetto si trova però nella cartella `scala` situata all'interno di `src/main`
```
├── src                  
    ├── main                
      ├── scala
          ├── apriori                 # Raccolta degli algoritmi Apriori
          ├── classes                 # Classi per le collezioni degli algoritmi FPGrowth
          ├── demo                    # Raccolta delle demo da mostrare all'esame
          ├── eclat                   # Raccolta degli algoritmi Eclat
          ├── fpgrowth                # Raccolta degli algoritmi FPGrowth
          ├── fpgrowthmod             # Raccolta della nostra versione degli algoritmi FPGrowth 
          ├── fpgrowthstar            # Raccolta degli algoritmi FPGrowthStar
          ├── mainClass               # Main dalla quale poter avviare le varie esecuzioni
          ├── nonord                  # Raccolta degli algoritmi NonOrdFPGrowth
          ├── utils                   # File scala nella quale sono contenute funzioni utili
```
All'interno di questi package troviamo tutte le implementazioni degli algoritmi e le varie classi utili alla creazione di nuove strutture dati per gli FPGrowth. Inoltre, nel package `mainClass`, sono presenti i due main principali dalla quale poter avviare gli algoritmi. In questi è anche possibile settare tutti i parametri dell'esecuzione come il _minimo supporto_ ed il _dataset_ sulla quale avviare l'algoritmo. 
* `MainClass.scala` è il file da avviare per l'esecuzione in locale;
* `MainClassGCP` è quello da utilizzare per l'esecuzione su Google Cloud Platform.



### Configurazione Google Cloud Platform:
 <!--  <details><summary> 0. Pre-requisiti:  </summary>
<p>
    Prima di iniziare la configurazione è necessario **creare un progetto** rinominato come _"ProgettoSCP"_ sul proprio profilo di **Google Cloud Platform**.      Assicurarsi inoltre di avere del **credito** per poter testare il tutto.
</p>
</details> -->

  #### 0. Pre-requisiti
  Prima di iniziare la configurazione è necessario **creare un nuovo progetto** sul proprio profilo di **Google Cloud Platform** e assicurarsi inoltre di avere del **credito** per poter testare il tutto. É importante notare che **tutti i passaggi** potranno essere svolti sia tramite **interfaccia** della GCP sia tramite **comandi da terminale**.

  #### 1. Creazione Bucket
  Per prima cosa bisogna creare il **bucket** nella quale andremo a mettere i dataset ed il jar che vogliamo eseguire. Successivamente verrà usato anche per salvare i risultati ottenuti dopo l'esecuzione.  
  Andiamo a creare un bucket chiamato _bucket-dataset-scp_ con _**Location type:** Region_ e _**Region:** europe-west3 (Frankfurt)_ , tutto il resto sarà lasciato di default.  
  Codice da eseguire sul terminale per avere il bucket con i parametri scritti in precedenza:
  
  ```
  gsutil mb -l EUROPE-WEST3 -b on gs://bucket-dataset-scp
  ```

  #### 2. Creazione Cluster
  Dopo aver creato il bucket è necessario creare il **cluster**. Questo sarà formato dalle macchine che eseguiranno il nostro codice. Rechiamoci dunque nella sezione _Dataproc_ e creiamolo. Nel nostro caso avevamo poco credito a disposizione, di conseguenza abbiamo optato per creare un cluster composto da **1 Master ed N Workers** (inizialmente 2 Workers). Parametri nello specifico:
  * **Cluster Name:** cluster-1
  * **Region:** europe-central2
  * **Zone:** europe-central2-a
  
  Tutto il resto è stato lasciato di default.
  
  Codice da eseguire sul terminale per avere il cluster con i parametri scritti in precedenza:
  ```
  gcloud dataproc clusters create cluster-1 --region europe-central2 --zone europe-central2-a --master-machine-type n1-standard-4 --master-boot-disk-size 500 --num-workers 2 --worker-machine-type n1-standard-4 --worker-boot-disk-size 500 --image-version 2.0-debian10
  ```

  #### 3. Avviare il cluster
  Dopo la creazione, il cluster dovrebbe attivarsi automaticamente. Se questo non dovesse succedere possiamo metterlo in esecuzione manualmente andando nella sezione **_Dataproc_**, selezionando **_cluster_1_** dalla lista dei nostri cluster e cliccando su **_Avvia_**.

  #### 4. Creare il .jar del nostro codice
  Utilizzando l'IDE IntelliJ andare nella sezione **_Terminale_** posta in basso ed eseguire il comando `sbt clean package` che andrà a creare il JAR composto solo dalle classi da eseguire in maniera distribuita. Troveremo il nostro archivio al seguente path: `target/scala-2.12/scp_dataminingproject_fp-growth_2.12-0.1.jar`.

  #### 5. Caricare il dataset ed il jar nel bucket
  Per poter creare il job è necessario caricare il dataset ed il jar all'interno del bucket creato in precedenza. Possiamo semplicemnte caricarli da interfaccia grafica recandoci nel bucket e cliccando su **_UPLOAD FILES_**. **Attenzione:** il dataset va caricato a sua volta all'interno di una cartella nominata `input`. Alla fine del procedimento avremo il nostro bucket composto in questo modo:
```
  ├── bucket-dataset-scp                  
      scp_dataminingproject_fp-growth_2.12-0.1.jar
      ├── input
          dataset.txt               # Sarà il dataset che abbiamo caricato
```
 
  #### 6. Creare un job con il nostro jar
  Per creare il Job ed eseguire la nostra applicazione abbiamo utilizzato direttamente il terminale. Le linee di comando utilizzate sono le seguenti:
  ```
 gsutil -m rm -r -f gs://bucket-dataset-scp/output/
gcloud dataproc jobs submit spark     --cluster=cluster-1     --class=mainClass.MainClassGCP     --jars=gs://bucket-dataset-scp/scp_dataminingproject_fp-growth_2.12-0.1.jar     --region=europe-central2     -- gs://bucket-dataset-scp/input/ gs://bucket-dataset-scp/output/ NonordFPRDD 0 30 10

  ```  
  Attraverso questo comando è anche possibile cambiare gli ultimi quattro parametri. Questi sono (in ordine):
  * **Nome algoritmo da eseguire**
    * AprioriRDD
    * EclatRDD
    * FPGrowthRDD
    * FPGrowthStarRDD
    * NonordFPRDD
    * FPGrowthModRDD 
  * **Dataset indicizzato**
    * 0 -> datasetKaggleAlimenti.txt
    * 1 -> T10I4D100K.txt
    * 2 -> T40I10D100K.txt
    * 3 -> mushroom.txt
    * 4 -> connect.txt
    * 5 -> datasetLettereDemo.txt
  * **Minimo supporto**
  * **Numero partizioni** (_Opzionale_, utile solo negli algoritmi di tipo FPGrowth. Se non inizializzato avrà valore **100 di default**.)
  
  
  Un altro modo per eseguire il comando scritto in precedenza è quello di caricare il file `execution.sh` (presente in questo repository) all'interno dell'editor della Google Cloud Platform come mostrato in figura:  
  
![upload-execution](https://user-images.githubusercontent.com/32647882/159300488-ea6a522f-6136-4724-a3ee-0b11f8621ca0.png)

Fatto questo sarà poi possibile modificare facilmente i vari parametri direttamente dall'editor.
Per eseguire questo script è necessario usare il comando:
```  
. execution.sh
```


  #### 7. Visualizzare il risultato
  Il passo finale sarà quello di andare a visualizzare i risultati ottenuti. Per fare ciò scriviamo sul terminale:

  ```
  gsutil cat gs://bucket-dataset-scp/output/${Nome algoritmo}Result/*
  ```
  dove dobbiamo sostituire a "_$Nome algoritmo_" l'algorimto che abbiamo eseguito.

