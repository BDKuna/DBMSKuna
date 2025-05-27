# DBMSKuna 📦🧠

<table>
<tr>
<td width="300px">
  <img src="https://github.com/user-attachments/assets/cf36fe01-1b66-4ecf-beae-f06a178f91ba" width="250px"/>
</td>
<td>
  <img src="https://github.com/user-attachments/assets/0bdaff2e-c4e2-4ec5-b518-9870d5d575b2" width="400px"/>
</td>
</tr>
</table>


**DBMSKuna** is a multimodal database management system developed for the course **CS2702 - DATABASE II** at UTEC.  
It integrates advanced file organization and indexing structures to support queries over structured and unstructured data using a custom SQL-like language and RESTful API.

---

## 🧠 Project Overview

DBMSKuna supports indexing techniques such as:

- 📂 **AVL File**
- 🧩 **ISAM** (2-level static index with overflow pages)
- 🧮 **Extendible Hashing**
- 🌳 **B+ Tree**
- 🗺️ **R-Tree** for spatial and multidimensional data
- 🎛️ **BRIN** (Legacy)

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

DBMSKuna supports any structured dataset in CSV format.

- Users can import their own data by specifying the CSV file path and defining the schema through the system’s SQL-like language.
- The engine automatically indexes the dataset using the selected indexing strategy (AVL, B+Tree, Hashing, etc.).
- Records are parsed and stored using binary serialization, with support for:
  - Integers, Floats, Strings (VARCHAR), Points (for spatial indexing). 


## 👥 Team Members

| Name                 | Email                            | GitHub User                               |
|----------------------|----------------------------------|--------------------------------------------|
| Eduardo Aragon       | eduardo.aragon@utec.edu.pe       | [EduardoAragon11](https://github.com/EduardoAragon11)|
| Jorge Quenta         | jorge.quenta@utec.edu.pe         | [jorge-qs](https://github.com/jorge-qs)     |
| Mikel Bracamonte     | mikel.bracamonte@utec.edu.pe              | [Mikel-Bracamonte](https://github.com/Mikel-Bracamonte)     |
| Sergio Lezama| sergio.lezama@utec.edu.pe              | [SergioSLO](https://github.com/SergioSLO)     |
| Jose Paca| jose.paca@utec.edu.pe              | [JFpro160](https://github.com/JFpro160)     |

## 📈 Results

We performed benchmarking tests on all implemented indexing structures using datasets of varying size and complexity.

### Metrics Evaluated:
- ⏱️ Execution Time (milliseconds)
- 📀 Disk Access Count (Read/Write Operations)

📊 You can view detailed performance plots and comparison charts on the [Results Wiki Page](https://github.com/BDKuna/DBMSKuna/wiki/Results).


## 📄 License

This project is licensed under the **MIT License**.

You are free to use, modify, and distribute this software in accordance with the terms of the MIT license.

🔗 See the [LICENSE](LICENSE) file for full details.

