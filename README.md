# DBMSKuna 📦🧠

![DBMSKuna Logo](ruta/a/tu/logo.png)

**DBMSKuna** is a multimodal database management system developed for the course **CS2702 - Base de Datos II** at UTEC.  
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
