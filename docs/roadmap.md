 
 Projet - Better Call SpaceLLM/
│
├── README.md
├── .gitignore
│
├── data/                          ← Phase 1 — Data Pipeline
│   ├── raw/                       # Documents bruts téléchargés
│   │   ├── en/
│   │   ├── fr/
│   │   ├── ru/
│   │   └── zh/
│   ├── processed/                 # Textes nettoyés et découpés
│   ├── embeddings/                # Vecteurs générés
│   └── sources.json               # Registre des sources (URL, date, langue)
│
├── rag/                           ← Phase 2 — RAG Core
│   ├── ingestion/
│   │   ├── loader.py              # Chargement des documents
│   │   ├── chunker.py             # Découpage en chunks
│   │   └── embedder.py            # Génération des embeddings
│   ├── retrieval/
│   │   ├── vector_store.py        # Interface ChromaDB
│   │   └── retriever.py           # Logique de recherche
│   ├── agent/
│   │   ├── agent.py               # Orchestration LangChain
│   │   ├── tools.py               # Outils de l'agent
│   │   └── prompts.py             # Templates de prompts
│   └── evaluation/
│       └── ragas_eval.py          # Métriques RAGAS
│
├── backend/                       ← Phase 4 — FastAPI
│   ├── main.py                    # Entry point FastAPI
│   ├── routers/
│   │   ├── query.py               # Route /query
│   │   └── documents.py           # Route /documents
│   ├── schemas/
│   │   └── models.py              # Pydantic models
│   └── requirements.txt
│
├── frontend/                      ← Phase 5 — React
│   ├── public/
│   └── src/
│       ├── components/
│       │   ├── ChatBox.jsx
│       │   ├── SourceCard.jsx
│       │   └── UploadDoc.jsx
│       ├── pages/
│       │   └── Home.jsx
│       ├── App.jsx
│       └── index.js
│
├── notebooks/                     # Expérimentations Jupyter
│   ├── 01_data_exploration.ipynb
│   ├── 02_embedding_tests.ipynb
│   └── 03_rag_prototype.ipynb
│
├── tests/                         # Tests unitaires
│   ├── test_chunker.py
│   ├── test_retriever.py
│   └── test_api.py
│
└── docs/                          # Documentation projet
    ├── architecture.md
    └── roadmap.md
