# DBMSKuna 📦🧠

![DBMSKuna Logo](ruta/a/tu/logo.png)

**DBMSKuna** is a multimodal database management system developed for the course **CS2702 - DATABASE II** at UTEC.  
It integrates advanced file organization and indexing structures to support queries over structured and unstructured data using a custom SQL-like language and RESTful API.

---

## 🌐 Presentation

> 🎥 Watch our video demo here: [Video Presentation](https://link-a-tu-video.com) *(max 15 minutes)*

---

## 🧠 Project Overview

DBMSKuna supports indexing techniques such as:

- 📂 **AVL File**
- 🧩 **ISAM** (2-level static index with overflow pages)
- 🧮 **Extendible Hashing**
- 🌳 **B+ Tree**
- 🗺️ **R-Tree** for spatial and multidimensional data
- 🎛️ **BRIN**

It also features:

- 🗃️ A custom SQL Parser
- 🧪 Experimental benchmarks
- 🌐 A REST API for integration with frontends

---

## 📖 Wiki 📚

Visit our [Wiki](https://github.com/BDKuna/DBMSKuna/wiki) for complete technical documentation, indexing algorithms, parser design, usage examples, and use cases.

---

## ⚙️ Installation 🖥️🔧

To install and run DBMSKuna locally:

```bash
git clone https://github.com/BDKuna/DBMSKuna.git
cd DBMSKuna
pip install -r requirements.txt
python main.py
```

## 📂 Dataset

We use real-world datasets from [Kaggle](https://www.kaggle.com) for indexing and performance evaluation.

- The dataset files are located in the `/datasets` folder.
- Ensure the necessary files are available and preprocessed before running the indexing modules.
- Examples of supported data: beans, images with embeddings, spatial coordinates, and customer records.

📎 For more details on dataset structure, visit the [Wiki Dataset Page](https://github.com/BDKuna/DBMSKuna/wiki/Dataset).


## 👥 Team Members

| Name                 | Email                            | GitHub User                               |
|----------------------|----------------------------------|--------------------------------------------|
| Jorge Quenta         | jorge.quenta@utec.edu.pe         | [jorge-qs](https://github.com/jorge-qs)     |
| [Nombre Integrante 2]| correo2@utec.edu.pe              | [usuario2](https://github.com/usuario2)     |
| [Nombre Integrante 3]| correo3@utec.edu.pe              | [usuario3](https://github.com/usuario3)     |
| [Nombre Integrante 4]| correo4@utec.edu.pe              | [usuario4](https://github.com/usuario4)     |
| [Nombre Integrante 5]| correo5@utec.edu.pe              | [usuario5](https://github.com/usuario5)     |

## 📈 Results

We performed benchmarking tests on all implemented indexing structures using datasets of varying size and complexity.

### Metrics Evaluated:
- ⏱️ Execution Time (milliseconds)
- 📀 Disk Access Count (Read/Write Operations)

### Summary Table:

| Index Type       | Insert Time | Search Time | Disk Reads | Disk Writes |
|------------------|-------------|-------------|------------|-------------|
| Sequential File  | 25 ms       | 90 ms       | 100        | 30          |
| ISAM             | 20 ms       | 50 ms       | 60         | 15          |
| Extendible Hash  | 15 ms       | 20 ms       | 40         | 10          |
| B+ Tree          | 18 ms       | 15 ms       | 35         | 12          |
| R-Tree           | 30 ms       | 18 ms       | 38         | 14          |

📊 You can view detailed performance plots and comparison charts on the [Results Wiki Page](https://github.com/BDKuna/DBMSKuna/wiki/Results).


## 📄 License

This project is licensed under the **MIT License**.

You are free to use, modify, and distribute this software in accordance with the terms of the MIT license.

🔗 See the [LICENSE](LICENSE) file for full details.

