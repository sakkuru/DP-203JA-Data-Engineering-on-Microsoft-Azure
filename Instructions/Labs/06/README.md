# モジュール 6 - Azure Databricks でのデータの探索と変換

このモジュールでは、さまざまな Apache Spark DataFrame メソッドを使用して、Azure Databricks でデータを探索して変換する方法を説明します。受講者は標準的な DataFrame メソッドを実行してデータの探索と変換を行う方法を学びます。また、重複データの削除や、日時の値の操作、列の名前変更、データの集計など、より高度なタスクを実行する方法を学習します。

このモジュールでは次のことができるようになります。

- Azure Databricks で DataFrames を使用してデータの探索とフィルタリングを行う
- 今後のクエリをより早く実行できるように DataFrame をキャッシュする
- 重複データを削除する
- 日付/時刻の値を操作する
- DataFrame 列を削除して名前を変更する
- DataFrame に格納されているデータを集計する

## ラボの詳細

- [モジュール 6 - Azure Databricks でのデータの探索と変換](#module-6---data-exploration-and-transformation-in-azure-databricks)
  - [ラボの詳細](#lab-details)
  - [ラボ 1 - DataFrame を使用する](#lab-1---working-with-dataframes)
    - [実践ラボの前](#before-the-hands-on-lab)
      - [タスク 1 - Azure Databricks ワークスペースを作成して構成する](#task-1---create-and-configure-the-azure-databricks-workspace)
    - [演習 1: ラボのノートブックを完了する](#exercise-1-complete-the-lab-notebook)
      - [タスク 1: Databricks アーカイブを複製する](#task-1-clone-the-databricks-archive)
      - [タスク 2: DataFrame の説明ノートブックを完了する](#task-2-complete-the-describe-a-dataframe-notebook)
    - [演習 2: DataFrame の使用ノートブックを完了する](#exercise-2-complete-the-working-with-dataframes-notebook)
    - [演習 3: 関数表示ノートブックを完了する](#exercise-3-complete-the-display-function-notebook)
    - [演習 4: 特徴的な記事の演習ノートブックを完了する](#exercise-4-complete-the-distinct-articles-exercise-notebook)
  - [ラボ 2 - DataFrame の高度なメソッドを使用する](#lab-2---working-with-dataframes-advanced-methods)
    - [演習 2: ラボのノートブックを完了する](#exercise-2-complete-the-lab-notebook)
      - [タスク 1: Databricks アーカイブを複製する](#task-1-clone-the-databricks-archive-1)
      - [タスク 2: 日付と時間の操作ノートブックを完了する](#task-2-complete-the-date-and-time-manipulation-notebook)
    - [演習 3: 集計関数の使用ノートブックを完了する](#exercise-3-complete-the-use-aggregate-functions-notebook)
    - [演習 4: データの重複排除の演習ノートブックを完了する](#exercise-4-complete-the-de-duping-data-exercise-notebook)

## ラボ 1 - DataFrame を使用する

データを読み取り処理するようにデータフレームを定義して、Azure Databricks でのデータ処理を実現します。このラボでは、Azure Databricks DataFrames を使用してデータを読み取る方法を説明します。Databricks Notebooks 内で演習を完了する必要があります。開始するには、Azure Databricks ワークスペースにアクセスできる必要があります。使用可能なワークスペースがない場合は、次の手順に従ってください。ワークスペースがある場合は、`Clone the Databricks archive`手順にスキップできます。

### 実践ラボの前

> **注:** ホストされたラボ環境を**使用しておらず**、ご自分の Azure サブスクリプションを使用している場合は、`Before the hands-on lab` の手順のみを完了してください。その他の場合は、演習 1 にスキップします。

このラボの演習を始める前に、利用可能なクラスターが含まれている Azure Databricks ワークスペースにアクセスできることを確認してください。以下のタスクを実行してワークスペースを構成します。

#### タスク 1 - Azure Databricks ワークスペースを作成して構成する

[ラボ 06 セットアップ方法](https://github.com/solliancenet/microsoft-data-engineering-ilt-deploy/blob/main/setup/06/lab-01-setup.md)に従い、ワークスペースを作成して構成します。

### 演習 1: ラボのノートブックを完了する

#### タスク 1: Databricks アーカイブを複製する

1. 現在 Azure Databricks ワークスペースを開いていない場合は、Azure portal で、デプロイ済みの Azure Databricks ワークスペースに移動し、「**ワークスペースの起動**」 を選択します。
1. 左側のウィンドウで、「**ワークスペース**」  >  「**ユーザー**」 の順に選択して、ご自分のユーザー名 (家のアイコンのエントリ) を選択します。
1. 表示されたウィンドウで、ご自分の名前の横にある矢印を選択し、「**インポート**」 を選択します。

    ![アーカイブをインポートするためのメニュー オプション](media/import-archive.png)

1. 「**ノートブックのインポート**」 ダイアログ ボックスで URL を選択し、次の URL 内に貼り付けます。

 ```
  https://github.com/solliancenet/microsoft-learning-paths-databricks-notebooks/blob/master/data-engineering/DBC/04-Working-With-Dataframes.dbc?raw=true
 ```

1. **インポート**を選択します。
1. 表示される **04-Working-With-Dataframes** フォルダーを選択します。

#### タスク 2: DataFrame の説明ノートブックを完了する

**1.Describe-a-dataframe** ノートブックを開きます。指示に従ってセルで操作を実行する前に、必ずクラスターをノートブックに接続してください。

ノートブック内で以下を行います。

- `DataFrame` API の知識を深める
- 次のクラスについて説明する
  - `SparkSession`
  - `DataFrame` (aka `Dataset[Row]`)
- 次のアクションについて説明する
  - `count()`

ノートブックを完了した後は、この画面に戻り、次のステップに進みます。

### 演習 2: DataFrame の使用ノートブックを完了する

Azure Databricks ワークスペースで、ユーザー フォルダー内にインポートした **04-Working-With-Dataframes** フォルダーを開きます。

**2.Use-common-dataframe-methods** ノートブックを開きます。指示に従ってセルで操作を実行する前に、必ずクラスターをノートブックに接続してください。

ノートブック内で以下を行います。

- `DataFrame` API の知識を深める
- パフォーマンスに共通のデータフレーム メソッドを使用する
- Spark API のドキュメントを探す

ノートブックを完了した後は、この画面に戻り、次のステップに進みます。

### 演習 3: 関数表示ノートブックを完了する

Azure Databricks ワークスペースで、ユーザー フォルダー内にインポートした **04-Working-With-Dataframes** フォルダーを開きます。

**3.Display-function** ノートブックを開きます。指示に従ってセルで操作を実行する前に、必ずクラスターをノートブックに接続してください。

ノートブック内で以下を行います。

- 次の変換について説明する
  - `limit(..)`
  - `select(..)`
  - `drop(..)`
  - `distinct()`
  - `dropDuplicates(..)`
- 次のアクションについて説明する
  - `show(..)`
  - `display(..)`

ノートブックを完了した後は、この画面に戻り、次のステップに進みます。

### 演習 4: 特徴的な記事の演習ノートブックを完了する

Azure Databricks ワークスペースで、ユーザー フォルダー内にインポートした **04-Working-With-Dataframes** フォルダーを開きます。

**4.Exercise:** ** Distinct Articles** ノートブックを開きます。指示に従ってセルで操作を実行する前に、必ずクラスターをノートブックに接続してください。

この演習では、Parquet ファイルを読み取り、必要な変換を適用して、レコードの合計数を実行し、すべてのデータが正しく読み込まれたことを確認します。おまけとして、データを照合するスキーマを定義してみて、このスキーマを使用するように読み取り操作を更新します。

> 注: 対応するノートブックは `Solutions` サブフォルダー内にあります。これには、演習用に完成されたセルが含まれています。行き詰まった場合、または単に解決方法を確認したい場合は、このノートブックを参照してください。

ノートブックを完了した後は、この画面に戻り、次のラボに進みます。

## ラボ 2 - DataFrame の高度なメソッドを使用する

このラボでは、上記のラボで学んだ Azure Databricks DataFrames のコンセプトに基づき、データ エンジニアが DataFrame を使用してデータの読み取り、書き込み、変換で使用できる高度なメソッドをいくつか確認します。

### 演習 2: ラボのノートブックを完了する

#### タスク 1: Databricks アーカイブを複製する

1. 現在 Azure Databricks ワークスペースを開いていない場合は、Azure portal で、デプロイ済みの Azure Databricks ワークスペースに移動し、「**ワークスペースの起動**」 を選択します。
1. 左側のウィンドウで、「**ワークスペース**」  >  「**ユーザー**」 の順に選択して、ご自分のユーザー名 (家のアイコンのエントリ) を選択します。
1. 表示されたウィンドウで、ご自分の名前の横にある矢印を選択し、「**インポート**」 を選択します。

    ![アーカイブをインポートするためのメニュー オプション](media/import-archive.png)

1. 「**ノートブックのインポート**」 ダイアログ ボックスで URL を選択し、次の URL 内に貼り付けます。

 ```
  https://github.com/solliancenet/microsoft-learning-paths-databricks-notebooks/blob/master/data-engineering/DBC/07-Dataframe-Advanced-Methods.dbc?raw=true
 ```

1. **インポート**を選択します。
1. 表示される **07-Dataframe-Advanced-Methods** フォルダーを選択します。

#### タスク 2: 日付と時間の操作ノートブックを完了する

**1.DateTime-Manipulation** ノートブックを開きます。指示に従ってセルで操作を実行する前に、必ずクラスターをノートブックに接続してください。

ノートブック内で以下を行います。

- `...Sql.functions` 操作の詳細を調べる
  - 日付と時刻の関数

ノートブックを完了した後は、この画面に戻り、次のステップに進みます。

### 演習 3: 集計関数の使用ノートブックを完了する

Azure Databricks ワークスペースで、ユーザー フォルダー内にインポートした **07-Dataframe-Advanced-Methods** フォルダーを開きます。

**2.Use-Aggregate-Functions** ノートブックを開きます。指示に従ってセルで操作を実行する前に、必ずクラスターをノートブックに接続してください。

ノートブック内では、さまざまな集計関数を学習します。

ノートブックを完了した後は、この画面に戻り、次のステップに進みます。

### 演習 4: データの重複排除の演習ノートブックを完了する

Azure Databricks ワークスペースで、ユーザー フォルダー内にインポートした **07-Dataframe-Advanced-Methods** フォルダーを開きます。

**3.Exercise-Deduplication-of-Data** ノートブックを開きます。指示に従ってセルで操作を実行する前に、必ずクラスターをノートブックに接続してください。

この演習の目的は、DataFrames の使い方について学習したことを、列の名前変更などで実践することです。作業を行うための空のセルと共に、ノートブック内に手順が記載されています。ノートブックの下部には、作業が正確であることを検証するのに役立つ追加のセルがあります。

> 注: 対応するノートブックは `Solutions` サブフォルダー内にあります。これには、演習用に完成されたセルが含まれています。行き詰まった場合、または単に解決方法を確認したい場合は、このノートブックを参照してください。
