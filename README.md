# 🧩 AWS Keyspaces Performance Benchmark (Amazon Reviews Dataset)

This project evaluates **Amazon Keyspaces (for Apache Cassandra)** using a real-world dataset and benchmarks both **single and bulk CRUD operations** using Python and CQL.

---

## 🔍 Overview

Amazon Keyspaces is a fully managed, serverless NoSQL database service built on **Apache Cassandra**. It supports **Cassandra Query Language (CQL)**, allowing teams to migrate relational and Cassandra-based workloads with minimal code changes.

### Key Features:
- ✅ **Serverless Architecture** — no infrastructure to manage, auto-scaling on demand  
- 🔐 **Secure by Default** — encryption at rest and in transit, IAM-based access  
- ⚡ **High Performance** — low-latency (<1 ms), handles millions of requests/sec  
- 🧰 **Cassandra-Compatible** — supports CQL, open-source drivers, and dev tools

---

## ⚙️ Project Setup

| Component        | Description                                          |
|------------------|------------------------------------------------------|
| **Keyspace**     | `all_beauty`                                         |
| **Table**        | `reviews`                                            |
| **Dataset**      | [Amazon Reviews 2023 – All Beauty](https://amazon-reviews-2023.github.io/) |
| **Records**      | 701,528 user reviews                                 |
| **Instance**     | EC2 `t3.medium`                                      |
| **Environment**  | Python 3.10, `virtualenv`                            |
| **Libraries**    | `boto3`, `cassandra-driver`, `cassandra-sigv4`       |

---

## 🔐 IAM-Based Keyspaces Connection

A secure connection is established using an **IAM user** with the `AmazonKeyspacesFullAccess` policy.  
- **SigV4AuthProvider** via `boto3`  
- **TLS Encryption** using `AmazonRootCA1.pem`  
- **Region**: `us-east-2`  

All operations are authenticated and encrypted end-to-end.

---

## 🚀 Main Runner Highlights

The main Python script:
- Creates the Keyspace and Table (if not exists)
- Executes **single-record CRUD operations** and logs timing
- Executes **bulk operations** (701,528 records) using:

```python
BATCH_SIZE = 200
MAX_WORKERS = 5
```

### 📊 Result

![Result](https://github.com/vishnu32510/keyspace-performance/blob/main/outputs/M_11_R1.png?raw=true)

### 🔹 Single Record CRUD Benchmark

| Operation | Type   | Records | Time (ms) | Time (s)   |
|-----------|--------|---------|-----------|------------|
| Insert    | Single | 1       | 17.48     | 0.01748    |
| Read      | Single | 1       | 18.45     | 0.01845    |
| Update    | Single | 1       | 15.37     | 0.01537    |
| Delete    | Single | 1       | 18.42     | 0.01842    |

> **Table 1:** Single-record CRUD benchmark on EC2 `t3.medium`


### 🔹 Bulk Record CRUD Benchmark (701,528 Records)

| Operation | Type | Records | Time (ms)   | Time (s)    |
|-----------|------|---------|-------------|-------------|
| Insert    | Bulk | 701,528 | 620,562.02  | 620.56202   |
| Read      | Bulk | 701,528 | 28,025.55   | 28.02555    |
| Update    | Bulk | 701,528 | 715,789.56  | 715.78956   |
| Delete    | Bulk | 701,528 | 716,322.56  | 716.32256   |

> **Table 2:** Bulk-record CRUD benchmark on EC2 `t3.medium` using batching and multithreading


### 🔹 Schema Operation Timings

| Operation       | Type   | Time (ms) | Time (s) |
|----------------|--------|-----------|----------|
| Create Keyspace | Schema | 23        | 0.023    |
| Create Table    | Schema | 74        | 0.074    |
| Drop Table      | Schema | 38        | 0.038    |

> **Table 3:** Amazon Keyspaces schema-level operation timing
