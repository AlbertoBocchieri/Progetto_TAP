Technologies for Advanced Programming
Autore: Alberto Bocchieri
Data: 2025-XX-XX

Introduzione
Questo progetto integra diverse tecnologie per realizzare un sistema completo per la ricerca, il download e il monitoraggio di torrent, con un arricchimento dei dati e un’interfaccia interattiva tramite un Bot Telegram. Le principali funzionalità includono scraping, streaming, indicizzazione, interazione, arricchimento e containerizzazione.

Architettura del Sistema
Il sistema è composto da diversi componenti, ciascuno containerizzato per garantire portabilità e scalabilità:

Scraping: Un'applicazione Python che esegue lo scraping dal sito KickassTorrent per estrarre informazioni sui torrent.
Streaming con Kafka: I dati raccolti vengono inviati a un topic Kafka per lo streaming in tempo reale.
Indicizzazione con Elasticsearch: I torrent vengono indicizzati in Elasticsearch per permettere ricerche e analisi rapide.
Bot Telegram: Un bot sviluppato in Python permette all’utente di interagire con il sistema per cercare torrent, avviare download e monitorare il progresso.
qBittorrent API: Il sistema interagisce con qBittorrent tramite la sua API per avviare e monitorare i download.
Apache Spark MLlib: Utilizza l'algoritmo ALS (Alternating Least Squares) per il collaborative filtering, permettendo di consigliare film basandosi sui feedback degli utenti.
Componenti del Progetto
Scraping e Gestione dei Torrent
Lo script di scraping utilizza le librerie requests e BeautifulSoup per:

Accedere al sito di KickassTorrent.
Estrarre titoli e magnet link dei torrent.
Filtrare i torrent in base a criteri di alta qualità (ad esempio, presenza di termini come 4K, 2160p, HDR, BDRemux, x265).
I dati raccolti vengono inviati come messaggi a un topic Kafka. Un consumer elabora questi dati e li indicizza in Elasticsearch.

Elasticsearch e Kafka
Elasticsearch è un motore di ricerca distribuito che gestisce le informazioni come documenti JSON, esposti tramite un'interfaccia RESTful. Nel progetto vengono creati due indici: uno per le informazioni sui torrent e uno per i feedback utente. Apache Kafka, invece, è una piattaforma di stream processing per l'elaborazione in tempo reale dei dati, utilizzata per la comunicazione tra gli script Python e Elasticsearch.

Bot Telegram e qBittorrent API
Il bot Telegram, sviluppato con python-telegram-bot, offre funzionalità quali:

Ricerca: /search permette di cercare torrent specificando il nome del film.
Download: L’utente può avviare il download tramite un pulsante inline che comunica con qBittorrent.
Monitoraggio: /monitor visualizza in tempo reale lo stato dei download, con pulsanti inline per fermare o riprendere singolarmente i torrent.
Consigli: /consigliami suggerisce un film in base ai feedback dell’utente.
Il bot interagisce con qBittorrent utilizzando la libreria qbittorrentapi per avviare i download tramite la sua API.

Docker e Orchestrazione
Il progetto è containerizzato tramite Docker. Elasticsearch, Kafka, Spark (master e worker) e altri componenti sono eseguiti in container separati. Docker Compose viene utilizzato per orchestrare e gestire i servizi, garantendo isolamento, scalabilità e portabilità.

Flusso di Lavoro del Sistema
Interazione dell'Utente: L’utente invia comandi al bot Telegram (ad es. /search o /consigliami).
Ricerca e Scraping: Il modulo di scraping estrae i torrent da KickassTorrent e li invia a Kafka.
Indicizzazione: I dati vengono processati e indicizzati in Elasticsearch.
Download: Tramite pulsanti inline, l’utente può avviare il download su qBittorrent.
Feedback: L’utente può fornire feedback sui torrent tramite pulsanti inline.
Monitoraggio: Il bot aggiorna periodicamente lo stato dei download e lo visualizza all’utente.
Interazione con l'Utente (Bot Telegram)
Il bot Telegram offre un'interfaccia interattiva con i seguenti comandi:

/start: Avvia il bot e mostra il menu dei comandi.
/search: Permette di cercare torrent specificando il nome del film.
/monitor: Avvia il monitoraggio in background dei download, aggiornando periodicamente lo stato.
/stop_monitor: Ferma il monitoraggio in background.
/consigliami: Suggerisce un film in base ai gusti dell'utente.
Inoltre, il bot utilizza pulsanti inline per avviare il download su qBittorrent e per fermare singolarmente i torrent attivi.



Arricchimento del Dato
Per rendere i dati più utili e completi, il sistema integra diverse fonti e metodi:

API TMDb: Per ottenere locandine, sinossi, cast, regista, anno di uscita e valutazioni dei film.
Classificazione automatica: Algoritmi che analizzano il titolo per determinare la qualità (ad es. presenza di termini come 4K, HDR, BDRemux).
Normalizzazione: Funzioni per rimuovere o standardizzare parti del titolo (ad es. rimuovendo il gruppo di rilascio).
Feedback dell'Utente: L'utente può fornire feedback tramite il bot per affinare le raccomandazioni.
Raccomandazioni: Il sistema utilizza Apache Spark MLlib e l'algoritmo ALS per implementare il collaborative filtering, permettendo di consigliare film basandosi sui feedback ricevuti.
Implementazione del Collaborative Filtering
Spark MLlib implementa l'algoritmo ALS (Alternating Least Squares) per costruire sistemi di raccomandazione. L'algoritmo scompone la matrice delle valutazioni in due matrici di fattori latenti: una che cattura le preferenze degli utenti e l'altra che rappresenta le caratteristiche dei film. Queste matrici vengono ottimizzate iterativamente, alternando problemi di regressione lineare, per dedurre i gusti degli utenti anche in assenza di valutazioni dirette. Questo approccio consente di identificare correlazioni tra utenti e film, generando raccomandazioni personalizzate. È importante notare che, per utenti con poche interazioni, il modello potrebbe soffrire del problema "cold start", con conseguenti raccomandazioni meno accurate.

Possibili Estensioni Future
Monitoraggio Avanzato: Integrazione di Grafana per visualizzazioni più sofisticate e alerting in tempo reale.
Analisi dei Contenuti: Utilizzo di strumenti NLP per estrarre ulteriori metadati dai torrent e arricchire il dato.
Sistema di Subscribe: Implementazione di un sistema di notifiche in tempo reale per i nuovi torrent caricati.
Conclusioni
L'integrazione di tecnologie come Kafka, Elasticsearch, Spark, Docker e un Bot Telegram consente di creare un sistema completo e scalabile per la gestione dei torrent. Oltre a raccogliere e indicizzare i torrent, il sistema li arricchisce con informazioni dettagliate e fornisce un'interfaccia interattiva e personalizzata per l'utente. Le possibilità di estensione sono molteplici e il sistema è progettato per essere modulare, facilitando l'integrazione di nuove funzionalità e miglioramenti futuri.
