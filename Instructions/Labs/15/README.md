# モジュール 15 - Event Hubs と Azure Databricks を使用してストリーム処理ソリューションを作成する

このラボでは、Azure Databricks で Event Hubs と Spark Structured Streaming を使用して大規模なストリーミング データの取り込みと処理を行う方法を学習します。構造化ストリーミングの主な機能と使用方法について学習します。スライディング ウィンドウを実装して、データのチャンクで集計を行い、基準値を適用して古いデータを削除します。最後に、Event Hubs に接続して、ストリームの読み取りと書き込みを行います。

このモジュールでは、次のことができるようになります。

- 構造化ストリーミングの主な機能と使用方法について把握する
- ファイルからデータをストリーミングし、分散ファイル システムに書き込みます
- スライディング ウィンドウを使用して、すべてのデータではなく、データのチャンクを集計します
- 基準値を適用して古いデータを削除する
- Event Hubs の読み取りおよび書き込みストリームに接続する

## ラボの詳細

- [モジュール 15 - Event Hubs と Azure Databricks を使用してストリーム処理ソリューションを作成する](#module-15---create-a-stream-processing-solution-with-event-hubs-and-azure-databricks)
  - [ラボの詳細](#lab-details)
  - [概念](#concepts)
  - [Event Hubs および Spark Structured Streaming](#event-hubs-and-spark-structured-streaming)
  - [ストリーミングの概念](#streaming-concepts)
  - [ラボ](#lab)
    - [実践ラボの前](#before-the-hands-on-lab)
      - [タスク 1: Azure Databricks ワークスペースを作成して構成する](#task-1-create-and-configure-the-azure-databricks-workspace)
    - [演習 1: Structured Streaming Concepts ノートブックを完了する](#exercise-1-complete-the-structured-streaming-concepts-notebook)
      - [タスク 1: Databricks アーカイブを複製する](#task-1-clone-the-databricks-archive)
      - [タスク 2: ノートブックを完了する](#task-2-complete-the-notebook)
    - [演習 2: Working with Time Windows ノートブックを完了する](#exercise-2-complete-the-working-with-time-windows-notebook)
    - [演習 3: Structured Streaming with Azure EventHubs ノートブックを完了する](#exercise-3-complete-the-structured-streaming-with-azure-eventhubs-notebook)

## 概念

Apache Spark Structured Streaming は、高速でスケーラブルなフォールト トレラント ストリーム処理 API です。ストリーミング データに対して、分析をほぼリアルタイムで実行するのに使用できます。

Strucrured Streaming では、SQL クエリを使用して、静的データの処理の場合と同じ方法でストリーミング データを処理できます。API では、最終データを継続的に増分および更新します。

## Event Hubs および Spark Structured Streaming

Azure Event Hubs は、瞬時に数百万ものデータを処理する、スケーラブルなリアルタイム データ インジェスト サービスです。複数のソースから大量のデータを受信し、準備されたデータを Azure Data Lake または Azure Blob storage にストリーミングすることができます。

Azure Event Hubs を Spark Structured Streaming と統合することで、メッセージをほぼリアルタイムで処理できます。Structured Streaming クエリと Spark SQL を使用して、処理されたデータのクエリと分析を行うことができます。

## ストリーミングの概念

ストリーム処理では、新しいデータを継続的に Data Lake Storage およびコンピューティング結果に組み込みます。従来のバッチ関連の処理手法を使用する場合、ストリーミング データはそれが処理される速度よりも速く取り込まれます。データ ストリームは、データが継続的に追加されるテーブルとして扱われます。そのようなデータの例として、銀行のカード決済、モノのインターネット (IoT) デバイス データ、およびビデオ ゲーム プレイ イベントなどがあります。

ストリーミング システムの構成要素は以下のとおりです。

- Kafka、Azure Event Hubs、IoT Hub、分散システム上のファイル、TCP-IP ソケットなどの入力ソース
- 構造化ストリーミング、forEach シンク、メモリ シンクなどを使用したストリーミング処理

## ラボ

Databricks Notebooks 内で演習を完了する必要があります。開始するには、Azure Databricks ワークスペースにアクセスできる必要があります。使用可能なワークスペースがない場合は、次の手順に従ってください。ワークスペースがある場合は、`Clone the Databricks archive` 手順にスキップできます。

### 実践ラボの前

> **注:** ホストされたラボ環境を**使用しておらず**、ご自分の Azure サブスクリプションを使用している場合は、`Before the hands-on lab` の手順のみを完了してください。その他の場合は、演習 1 にスキップします。

このラボの演習を始める前に、利用可能なクラスターが含まれている Azure Databricks ワークスペースにアクセスできることを確認してください。以下のタスクを実行してワークスペースを構成します。

#### タスク 1: Azure Databricks ワークスペースを作成して構成する

[ラボ 15 セットアップ方法](https://github.com/solliancenet/microsoft-data-engineering-ilt-deploy/blob/main/setup/15/lab-01-setup.md)に従い、ワークスペースを作成して構成します。

### 演習 1: Structured Streaming Concepts ノートブックを完了する

#### タスク 1: Databricks アーカイブを複製する

1. 現在 Azure Databricks ワークスペースを開いていない場合は、Azure portal で、デプロイ済みの Azure Databricks ワークスペースに移動し、「**ワークスペースの起動**」 を選択します。
1. 左側のウィンドウで、「**ワークスペース**」  >  「**ユーザー**」 の順に選択して、ご自分のユーザー名 (家のアイコンのエントリ) を選択します。
1. 表示されたウィンドウで、ご自分の名前の横にある矢印を選択し、「**インポート**」 を選択します。

    ![アーカイブをインポートするためのメニュー オプション](media/import-archive.png)

1. 「**ノートブックのインポート**」 ダイアログ ボックスで URL を選択し、次の URL 内に貼り付けます。

 ```
  https://github.com/solliancenet/microsoft-learning-paths-databricks-notebooks/blob/master/data-engineering/DBC/10-Structured-Streaming.dbc?raw=true
 ```

1. **インポート**を選択します。
1. 表示される **10-Structured-Streaming** フォルダーを選択します。

#### タスク 2: ノートブックを完了する

**1.Structured-Streaming-Concepts** ノートブックを開きます。指示に従ってセルで操作を実行する前に、クラスターをノートブックに接続していることを確認してください。

ノートブック内で以下を行います。

- ファイルからデータをストリーミングし、分散ファイル システムに書き込みます
- アクティブなストリームを一覧表示します
- アクティブなストリームを停止します

ノートブックを完了した後は、この画面に戻り、次のステップに進みます。

### 演習 2: Working with Time Windows ノートブックに記入する

Azure Databricks ワークスペースで、ユーザー フォルダー内にインポートした **10-Structured-Streaming** フォルダーを開きます。

**2.Time-Windows** ノートブックを開きます。指示に従ってセルで操作を実行する前に、クラスターをノートブックに接続していることを確認してください。

ノートブック内で以下を行います。

- スライディング ウィンドウを使用して、すべてのデータではなく、データのチャンクを集計します
- 基準値を適用して、保持する領域がない失効した古いデータを破棄します
-  `display` を使用してライブ グラフをプロットします

ノートブックを完了した後は、この画面に戻り、次のステップに進みます。

### 演習 3: Structured Streaming with Azure EventHubs ノートブックに記入する

Azure Databricks ワークスペースで、ユーザー フォルダー内にインポートした **10-Structured-Streaming** フォルダーを開きます。

**3.Streaming-With-Event-Hubs-Demo** ノートブックを開きます。指示に従ってセルで操作を実行する前に、クラスターをノートブックに接続していることを確認してください。

ノートブック内で以下を行います。

- Event Hubs に接続し、ご使用のイベント ハブにストリームを書き込みます
- イベント ハブからストリームを読み取ります
- JSON ペイロードのスキーマを定義し、データを解析してテーブル内に表示します
