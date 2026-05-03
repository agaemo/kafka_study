# Kafka 学習リポジトリ

イベント駆動アーキテクチャの観点からKafkaを体系的に学ぶためのリポジトリです。
理論ドキュメントとDockerを使ったハンズオンをセットで提供しています。

> このリポジトリは [Claude Code](https://claude.ai/code) を使って作成しました。
> 内容の正確性には注意を払っていますが、必ず公式ドキュメントと合わせて確認してください。

---

## 必要なもの

| ツール | 用途 |
|---|---|
| [Docker Desktop](https://www.docker.com/products/docker-desktop/) | コンテナ環境の実行 |
| Docker Compose v2 | 複数コンテナの管理（Docker Desktop に同梱） |
| [mise](https://mise.jdx.dev/) | Python・uvのバージョン管理 |
| [uv](https://docs.astral.sh/uv/) | Pythonパッケージ管理 |

---

## セットアップ

**ステップ1：KafkaをDockerで起動する**

```bash
git clone https://github.com/agaemo/kafka_study.git my_kafka_study
cd my_kafka_study
docker compose up -d
```

Kafka UI（<http://localhost:8080>）でKafkaの状態を視覚的に確認できます。

**ステップ2：Python・uvをmiseでインストールし、パッケージを入れる**

ハンズオンのPythonスクリプトはローカルで実行し、`localhost:9092` 経由でDockerのKafkaに接続します。

```bash
mise trust     # .mise.toml を信頼する（初回のみ）
mise install   # Python・uv をインストール
uv sync        # 依存パッケージをインストール
source .venv/bin/activate  # 仮想環境を有効化
```

以降のPythonスクリプトはすべて仮想環境内で実行します。ターミナルを開き直した場合は `source .venv/bin/activate` を再実行してください。

---

## 終了・片付け

```bash
deactivate         # 仮想環境から抜ける
docker compose down
```

| コマンド | 動作 |
|---|---|
| `deactivate` | 仮想環境から抜ける |
| `docker compose stop` | コンテナを停止するだけ（次回 `up` で再開できる） |
| `docker compose down` | コンテナとネットワークを削除する（イメージは残る） |
| `docker compose down --rmi all --volumes` | イメージとボリュームも含めて完全削除 |

---

## 学習フェーズ

### はじめに：Kafkaとは何か

Kafkaの構成・主要概念・他ツールとの比較を理解する。

- Kafkaに外部DBが不要な理由
- Topic / Producer / Consumer / Partition / Consumer Group / Offset の関係
- Redis・RabbitMQとの違いとKafkaを選ぶ理由

→ [教材を読む](intro/README.md)

---

### フェーズ1：基本的な送受信

ProducerとConsumerを実装し、メッセージの送受信の基本を体験する。

- KafkaをDockerで起動する
- PythonでProducer/Consumerを実装する
- Kafka UIでTopicとメッセージを確認する
- Consumer再起動後に続きから読めることを確認する

→ [教材とハンズオン](phase1/README.md)

---

### フェーズ2：Consumer Group

複数のConsumerで同一Topicを分担して処理する仕組みを体験する。

- Consumer Groupによる負荷分散
- PartitionとConsumerの割り当て関係
- Consumer障害時のRebalance
- 異なるGroupによる独立した読み取り

→ [教材とハンズオン](phase2/README.md)

---

### フェーズ3：Partitionとオフセット管理

キーによるPartition振り分けと、オフセットを使った再処理を体験する。

- キーによる順序保証
- オフセットの自動管理と手動管理
- 障害後の再処理フロー

→ [教材とハンズオン](phase3/README.md)

---

### ベストプラクティス

本番環境で安全に運用するために知っておくべきことを整理する。

- 冪等性の設計とDead Letter Topic
- オフセットのコミット戦略
- Partition数の設計指針
- 監視すべき指標（Consumer Lag など）

→ [教材を読む](best_practices/README.md)

---

## 学習を終えて

Kafkaの核心は「**イベントをログとして永続化し、誰でも何度でも読める**」という思想にあります。
メッセージを送った側は「誰が読むか・いつ読むか」を気にしない。受け取る側は自分のペースで処理し、失敗したら再処理できる。
この分離こそが、Kafkaが大規模なイベント駆動システムで使われる理由です。

---

## ディレクトリ構成

```
.
├── docker-compose.yml         # Kafka + Kafka UI
├── requirements.txt           # Python依存パッケージ
├── intro/README.md            # はじめに（教材）
├── phase1/                    # 基本的な送受信
│   ├── README.md
│   ├── producer.py
│   └── consumer.py
├── phase2/                    # Consumer Group
│   ├── README.md
│   ├── producer.py
│   ├── consumer.py
│   └── consumer_another_group.py
├── phase3/                    # Partitionとオフセット管理
│   ├── README.md
│   ├── producer_with_key.py
│   ├── consumer_normal.py
│   └── consumer_seek.py
└── best_practices/README.md   # ベストプラクティス
```

---

## 参考資料

| 資料 | 説明 |
|---|---|
| [Apache Kafka公式ドキュメント](https://kafka.apache.org/documentation/) | アーキテクチャ・設定リファレンス |
| [kafka-python](https://kafka-python.readthedocs.io/) | 使用しているPythonクライアントのドキュメント |
| [Kafka UI](https://github.com/provectuslabs/kafka-ui) | 使用しているGUI管理ツール |
