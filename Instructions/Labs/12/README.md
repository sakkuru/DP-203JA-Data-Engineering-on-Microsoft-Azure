# モジュール 12 - Azure Synapse Link を使用してハイブリッド トランザクション分析処理 (HTAP) に対応する

このモジュールでは、受講者はAzure Synapse Link によって Azure Cosmos DB アカウントを Synapse ワークスペースにシームレスに接続する方法を学びます。Synapse リンクを有効にして構成する方法、および Apache Spark プールと SQL サーバーレス プールを使用して Azure Cosmos DB 分析ストアのクエリを行う方法を学習します。

このモジュールでは、次のことができるようになります。

- Azure Cosmos DB を使用して Azure Synapse Link を構成する
- Synapse Analytics で Apache Spark を使用して Azure Cosmos DB に対してクエリを実行する
- Azure Synapse Analytics の サーバーレス SQL プールを使用して Azure Cosmos DB に対するクエリを実行する

## ラボの詳細

- [モジュール 12 - Azure Synapse Link を使用してハイブリッド トランザクション分析処理 (HTAP) に対応する](#module-12---support-hybrid-transactional-analytical-processing-htap-with-azure-synapse-link)
  - [ラボの詳細](#lab-details)
  - [ラボの構成と前提条件](#lab-setup-and-pre-requisites)
  - [演習 1: ラボの構成](#exercise-1-lab-setup)
    - [タスク 1: リンクされたサービスの作成](#task-1-create-linked-service)
    - [タスク 2: データセットを作成する](#task-2-create-dataset)
  - [演習 2: Azure Cosmos DB を使用して Azure Synapse Link を構成する](#exercise-2-configuring-azure-synapse-link-with-azure-cosmos-db)
    - [タスク 1: Azure Synapse Link を有効にする](#task-1-enable-azure-synapse-link)
    - [タスク 2: 新しい Azure Cosmos DB コンテナーを作成します。](#task-2-create-a-new-azure-cosmos-db-container)
    - [タスク 3: COPY パイプラインを作成して実行する](#task-3-create-and-run-a-copy-pipeline)
  - [演習 3: Synapse Analytics で Apache Spark を使用して Azure Cosmos DB に対してクエリを実行する](#exercise-3-querying-azure-cosmos-db-with-apache-spark-for-synapse-analytics)
    - [タスク 1: ノートブックを作成する](#task-1-create-a-notebook)
  - [演習 4: Azure Synapse Analytics の サーバーレス SQL プールを使用して Azure Cosmos DB に対するクエリを実行する](#exercise-4-querying-azure-cosmos-db-with-serverless-sql-pool-for-azure-synapse-analytics)
    - [タスク 1: 新しい SQL スクリプトを作成する](#task-1-create-a-new-sql-script)

## ラボの構成と前提条件

> **注:** ホストされたラボ環境を**使用しておらず**、ご自分の Azure サブスクリプションを使用している場合は、`Lab setup and pre-requisites` の手順のみを完了してください。その他の場合は、演習 1 にスキップします。

このモジュールの**[ラボの構成手順](https://github.com/solliancenet/microsoft-data-engineering-ilt-deploy/blob/main/setup/04/README.md)を完了**してください。

以下のモジュールは、同じ環境を共有している点に留意してください。

- [モジュール 4](labs/04/README.md)
- [モジュール 5](labs/05/README.md)
- [モジュール 7](labs/07/README.md)
- [モジュール 8](labs/08/README.md)
- [モジュール 9](labs/09/README.md)
- [モジュール 10](labs/10/README.md)
- [モジュール 11](labs/11/README.md)
- [モジュール 12](labs/12/README.md)
- [モジュール 13](labs/13/README.md)
- [モジュール 16](labs/16/README.md)

## 演習 1: ラボの構成

### タスク 1: リンクされたサービスの作成

以下の手順で、Azure Cosmos DB リンク サービスを作成してください。

> **注**: すでに、以前のモジュールでこの環境内で以下を作成している場合、このセクションはスキップしてください。
> 
> リンク サービス:
> - `asacosmosdb01` (Cosmos DB)
> 
> 統合データセット:
> - `asal400_customerprofile_cosmosdb`

1. Synapse Studio (<https://web.azuresynapse.net/>) を開き、「**管理**」 ハブまでナビゲートします。

    ![管理メニュー項目が強調表示されています。](media/manage-hub.png "Manage hub")

2. 「**リンク サービス**」 を開き、「**+ 新規**」 を選択して新しいリンク サービスを作成します。オプションのリストで 「**Azure Cosmos DB (SQL API)**」 を選択し、「**続行**」 を選択します。

    ![「管理」、「新規」、[Azure Cosmos DB リンク サービス」 のオプションが強調表示されています。](media/create-cosmos-db-linked-service-step1.png "New linked service")

3. リンク サービスに `asacosmosdb01` **(1)** という名前を付け、「**Cosmos DB アカウント名**」 (`asacosmosdbSUFFIX`) を選択して 「**データベース名**」 の値を `CustomerProfile` **(2)** に設定します。「**テスト接続**」 を選択して接続成功を確認してから **(3)**、「**作成**」 (4) を選択します。

    ![新しい Azure Cosmos DB リンク サービス。](media/create-cosmos-db-linked-service.png "New linked service")

### タスク 2: データセットを作成する

以下の手順を完了して、`asal400_customerprofile_cosmosdb` データセットを作成してください。

> **プレゼンターへのメモ**: すでにモジュール 4 を完了している場合は、このセクションをスキップしてください。

1. 「**データ**」 ハブに移動します。

    ![データ メニュー項目が強調表示されています。](media/data-hub.png "Data hub")

2. ツールバーで **+** を選択し **(1)**、「**統合データセット**」 (2) を選択して新しいデータセットを作成します。

    ![新しいデータセットを作成します。](media/new-dataset.png "New Dataset")

3. リストから 「**Azure Cosmos DB (SQL API)**」 **(1)** を選択し、「**続行**」 (2) を選択します。

    ![Azure Cosmos DB SQL API オプションが強調表示されています。](media/new-cosmos-db-dataset.png "Integration dataset")

4. 以下の特徴でデータセットを構成し、「**OK**」 (4) を選択します。

    - **名前**: `asal400_customerprofile_cosmosdb` **(1)** と入力します。
    - **リンク サービス**: Azure Cosmos DB リンク サービス **(2)** を選択します。
    - **コレクション**: `OnlineUserProfile01` **(3)** を選択します。

    ![新しい Azure Cosmos DB データセット。](media/create-cosmos-db-dataset.png "New Cosmos DB dataset")

5. データセットの作成後、「**接続**」 タブで 「**データのプレビュー**」 を選択します。

    ![データセットの 「データのプレビュー」 ボタンが強調表示されています。](media/cosmos-dataset-preview-data-link.png "Preview data")

6. データのプレビューで、選択された Azure Cosmos DB コレクションのクエリを実行し、その内部でドキュメントのサンプルを返します。ドキュメントは JSON 形式で格納され、`userId` フィールド、`cartId`、`preferredProducts` (空の可能性がある製品 ID の配列)、`productReviews` (空の可能性がある書き込み済みの製品レビューの配列) が含まれています。

    ![Azure Cosmos DB データのプレビューが表示されます。](media/cosmos-db-dataset-preview-data.png "Preview data")

7. 「**すべて公開**」 を選択した後、**公開**して新しいリソースを保存します。

    ![「すべて公開」 が強調表示されています。](media/publish-all-1.png "Publish all")

## 演習 2: Azure Cosmos DB を使用して Azure Synapse Link を構成する

Tailwind Traders では Azure Cosmos DB を使用して、電子商取引サイトからユーザー プロファイル データを格納しています。Azure Cosmos DB SQL API によって提供される NoSQL ドキュメント ストアでは、使い慣れた SQL 構文を使用してデータを管理する一方で、ファイルの読み取りと書き込みは大規模なグローバル スケールで行うことができます。

Tailwind Traders では Azure Cosmos DB の機能やパフォーマンスに満足していますが、データ ウェアハウスから複数のパーティションにわたる大量の分析クエリ (クロスパーティション クエリ) を実行するコストについて懸念しています。Azure Cosmos DB の要求ユニット (RU) を増やすことなく、すべてのデータに効率的にアクセスする必要があります。彼らは、Azure Cosmos DB 変更フィード メカニズムを使用して、変更時にコンテナーからデータ レイクにデータを抽出するためのオプションに注目しました。このアプローチの問題は、追加のサービスとコードの依存関係、およびソリューションの長期的なメンテナンスです。Synapse パイプラインから一括エクスポートを実行できますが、任意の時点での最新情報は得られません。

Cosmos DB 用の Azure Synapse Link を有効にし、Azure Cosmos DB コンテナー上で分析ストアを有効にすることに決めます。この構成では、すべてのトランザクション データが完全に分離された列ストアに自動的に格納されます。このストアを使用すると、トランザクション ワークロードに影響を与えたり、リソース ユニット (RU) コストを発生させたりすることなく、Azure Cosmos DB 内のオペレーショナル データに対して大規模な分析を実行できます。Cosmos DB 用の Azure Synapse Link では、Azure Cosmos DB と Azure Synapse Analytics 間に緊密な統合が作成されます。これにより、Tailwind Traders は、ETL なしでのオペレーショナル データに対するほぼリアルタイムの分析と、トランザクション ワークロードからの完全なパフォーマンスの分離を実行できます。

Azure Synapse Link では、Cosmos DB のトランザクション処理の分散スケールと、Azure Synapse Analytics の組み込みの分析ストアおよびコンピューティング能力を組み合わせることにより、Tailwind Trader のビジネス プロセスを最適化するためのハイブリッド トランザクション/分析処理 (HTAP) アーキテクチャが有効になります。この統合により、ETL プロセスが不要になり、ビジネス アナリスト、データ エンジニア、データ サイエンティストがセルフサービスを実現し、ほぼリアルタイムの BI、分析、および Machine Learning パイプラインをオペレーショナル データに対して実行できるようになります。

### タスク 1: Azure Synapse Link を有効にする

1. Azure portal (<https://portal.azure.com>) に移動し、ラボ環境のリソース グループを開きます。

2. 「**Azure Cosmos DB アカウント**」 を選択します。

    ![Azure Cosmos DB アカウントが強調表示されます。](media/resource-group-cosmos.png "Azure Cosmos DB account")

3. 左側のメニューで 「**機能**」 **(1)** を選択し、「**Azure Synapse リンク**」 (2)を選択します。

    ![機能ブレードが表示されます。](media/cosmos-db-features.png "Features")

4. 「**有効化**」 を選択します。

    ![「有効化」 が強調表示されます。](media/synapse-link-enable.png "Azure Synapse Link")

    分析ストアを使用して Azure Cosmos DB コンテナーを作成する前に、まず Azure Synapse Link を有効にする必要があります。

5. 続行する前に、この操作が完了するのを待ってください。1 分ほどかかります。Azure **通知**アイコンを選択して、ステータスをチェックします。

    ![Synapse リンクの有効化プロセスが実行中です。](media/notifications-running.png "Notifications")

    完了すると、「Synapse リンクの有効化」 の隣に緑色のチェックマークが表示されます。

    ![操作が正常に完了しました。](media/notifications-completed.png "Notifications")

### タスク 2: 新しい Azure Cosmos DB コンテナーを作成します。

Tailwind Traders には、`OnlineUserProfile01` という名前の Azure Cosmos DB コンテナーがあります。コンテナーが既に作成された_後_に Azure Synapse Link 機能が有効になったため、コンテナーで分析ストアを有効にすることはできません。同じパーティション キーを持つ新しいコンテナーを作成し、分析ストアを有効にします。

コンテナーを作成した後、新しい Synapse パイプラインを作成して、`OnlineUserProfile01` コンテナーから新しいコンテナーにデータをコピーします。

1. 左側のメニューで 「**データ エクスプローラー**」 を選択します。

    ![メニュー項目が選択されています。](media/data-explorer-link.png "Data Explorer")

2. **新しいコンテナー**を選択します。

    ![ボタンが強調表示されています。](media/new-container-button.png "New Container")

3. 「**データベース ID**」 で、「**既存のものを使用**」 を選択し、**`CustomerProfile` (1)** を選択します。「**コンテナー ID**」 (2) に **`UserProfileHTAP`** と入力し、「**パーティション キー**」 (3) に **`/userId`** と入力します。「**スループット**」については、「**自動スケール**」 (4) を選択し、「**最大 RU/秒**」 の値 **(5)** に **`4000`** と入力します。最後に、「**分析ストア**」 を 「**オン**」 (6) に設定し、「**OK**」 を選択します。

    ![説明されたようにフォームが設定されています。](media/new-container.png "New container")

    ここでは、`partition key` の値を `customerId` に設定しています。これはクエリで最も頻繁に使用するフィールドで、パーティション分割のパフォーマンスを向上させるために比較的高いカーディナリティ (一意の値の数) が含まれているためです。スループットを 「自動スケール」 にし、最大値は 4,000 要求ユニット (RU) に設定します。これは、コンテナーに割り当てられた最小値が 400 RU (最大数の 10%) であり、スケール エンジンがスループットの増加を保証するために十分な量の要求を検出した場合に最大 4,000 までスケールアップすることを意味します。最後に、コンテナーで**分析ストア**を有効にします。これにより、Synapse Analytics 内からハイブリッド トランザクション/分析処理 (HTAP) アーキテクチャを最大限に活用することができます。

    ここでは、新しいコンテナーにコピーするデータを簡単に見てみましょう。

4. 「**CustomerProfile**」 データベースの下にある `OnlineUserProfile01` コンテナーを展開し、「**項目**」 (1) を選択します。ドキュメントの 1 つ **(2)** を選択し、その内容を表示します **(3)**。ドキュメントは JSON 形式で格納されます。

    ![コンテナー項目が表示されます。](media/existing-items.png "Container items")

5. 左側のメニューで 「**キー**」 **(1)** を選択し、**主キー**の値 **(2)** をコピーしてメモ帳などに保存し、後ほど参照できるようにします。左上コーナーで Azure Cosmos DB **アカウント名** **(3)** をコピーし、メモ帳などのテキスト エディターに保存して後ほど使用できるようにします。

    ![主キーが強調表示されています。](media/cosmos-keys.png "Keys")

    > **注**: これらの値を書き留めておきます。この情報は、デモの最後の方で SQL ビューを作成する際に必要になります。

### タスク 3: COPY パイプラインを作成して実行する

新しい Azure Cosmos DB コンテナーで分析ストアが有効になったので、Synapse Pipeline を使用して既存のコンテナーの内容をコピーする必要があります。

1. Synapse Studio (<https://web.azuresynapse.net/>) を開き、「**統合**」 ハブまでナビゲートします。

    ![統合メニュー項目が強調表示されています。](media/integrate-hub.png "Integrate hub")

2. **+ (1)** を選択した後、**パイプライン (2)** を選択します。

    ![新しいパイプラインのリンクが強調表示されています。](media/new-pipeline.png "New pipeline")

3. 「アクティビティ」 で `Move & transform` グループを展開し、「**データのコピー**」 アクティビティをキャンバスにドラッグします **(1)**。「プロパティ」 ブレードで 「**名前**」 を **`Copy Cosmos DB Container`** に設定します **(2)**。

    ![新しいコピー アクティビティが表示されます。](media/add-copy-pipeline.png "Add copy activity")

4. キャンバスに追加した新しいコピー アクティビティを選択し、「**ソース**」 タブ **(1)** を選択します。リストから **`asal400_customerprofile_cosmosdb`** ソース データセットを選択します **(2)**。

    ![ソースが選択されています。](media/copy-source.png "Source")

5. 「**シンク**」 タブ **(1)** を選択した後、「**+ 新規**」 (2) を選択します。

    ![シンクが選択されています。](media/copy-sink.png "Sink")

6. 「**Azure Cosmos DB (SQL API)**」 データ型 **(1)** を選択し、「**続行**」 (2) を選択します。

    ![Azure Cosmos DB が選択されています。](media/dataset-type.png "New dataset")

7. 「**名前**」 で **`cosmos_db_htap` (1)** と入力します。**`asacosmosdb01` (2)** **リンク サービス**を選択します。**`UserProfileHTAP` (3)** **コレクション**を選択します。**スキーマのインポート** (4) で 「**接続/ストアから**」 を選択し、「**OK**」 (5) を選択します。

    ![説明されたようにフォームが設定されています。](media/dataset-properties.png "Set properties")

8. 追加したばかりの新しいシンク データセットで 「**挿入**」 書き込み操作を選択します。

    ![シンク タブが表示されます。](media/sink-insert.png "Sink tab")

9. 「**すべて公開**」 を選択した後、**公開**して新しいパイプラインを保存します。

    ![すべてを公開します。](media/publish-all-1.png "Publish")

10. パイプライン キャンバスの上で 「**トリガーの追加**」 (1) を選択した後、「**今すぐトリガー**」 (2) を選択します。「**OK**」 を選択して実行をトリガーします。

    ![トリガー メニューが表示されています。](media/pipeline-trigger.png "Trigger now")

11. 「**監視**」 ハブに移動します。

    ![監視ハブ。](media/monitor-hub.png "Monitor hub")

12. 「**パイプライン実行**」 (1) を選択し、パイプラインの実行が完了するまで待ちます **(2)**。「**更新**」 (3) を数回選択する必要があるかもしれません。

    ![パイプライン実行が完了として表示されています。](media/pipeline-run-status.png "Pipeline runs")

    > この操作が完了するには、**4 分程度**かかる可能性があります。これを実行している間に、ラボの手順の残りを読み、内容をよく理解しておいてください。

## 演習 3: Synapse Analytics で Apache Spark を使用して Azure Cosmos DB に対してクエリを実行する

Tailwind Traders は Apache Spark を使用し、新しい Azure Cosmos DB コンテナーに対して分析クエリを実行したいと考えています。このセグメントでは、Synapse Studio の組み込みジェスチャを使用して、トランザクション ストアに影響を与えることなく、Synapse ノートブックをすばやく作成し、HTAP が有効になったコンテナーの分析ストアからデータを読み込みます。

Tailwind Traders は各ユーザーで特定されたお気に入りの製品リストを、レビュー履歴で一致する製品 ID に組み合わせて利用し、すべてのお気に入り製品レビューのリストを表示しようとしています。

### タスク 1: ノートブックを作成する

1. 「**データ**」 ハブに移動します。

    ![データ ハブ](media/data-hub.png "Data hub")

2. 「**リンク済み**」 タブ **(1)** を選択し、**Azure Cosmos DB** セクションを展開してから **asacosmosdb01 (CustomerProfile)** リンク サービス **(2)** を展開します。**UserProfileHTAP** コンテナー **(3)** を右クリックし、「**新しいノートブック**」 ジェスチャ **(4)** を選択してから 「**DataFrame に読み込む**」 (5) を選択します。

    ![新しいノートブック ジェスチャが強調表示されています。](media/new-notebook.png "New notebook")

    作成した `UserProfileHTAP` コンテナーはのアイコンは、他のコンテナーのアイコンとわずかに異なっていることがわかります。これは、分析ストアが有効になっていることを示します。

3. 新しいノートブックで 「**アタッチ先**」 ドロップダウン リストから Spark プールを選択します。

    ![アタッチ先ドロップダウン リストが強調表示されています。](media/notebook-attach.png "Attach the Spark pool")

4. 「**すべて実行**」 (1) を選択します。

    ![新しいノートブックがセル 1 の出力とともに表示されています。](media/notebook-cell1.png "Cell 1")

    > 初めて Spark セッションを開始する際は数分かかります。

    セル 1 内で生成されたコードでは、`spark.read` 形式が **`cosmos.olap` (2)** に設定されていることがわかります。これにより、Synapse リンクはコンテナー分析ストアを使用するよう指示されます。トランザクション ストアに接続したい場合は (変更フィードから読み取ったり、コンテナーに書き込んだりするなど)、`cosmos.oltp` を使用します。

    > **注:** 分析ストアに書き込むことはできず、読み取りのみが可能です。コンテナーにデータを読み込みたい場合は、トランザクション ストアに接続する必要があります。

    最初の `option` は Azure Cosmos DB リンク サービスの名前を構成します **(3)**。2 番目の `option` は、読み取りたい Azure Cosmos DB コンテナーを定義します **(4)**。

5. 実行したセルの下で **+** ボタンを選択し、「**</> コード セル**」 を選択します。これにより、最初のコード セルの下に新しいコード セルが追加されます。

    ![コードの追加ボタンが強調表示されています。](media/add-code.png "Add code")

6. DataFrame には不要な追加の列が含まれています。不要な列を削除し、DataFrame のクリーンなバージョンを作成してみましょう。そのためには、新しいセルに次のように入力して**実行**します。

    ```python
    unwanted_cols = {'_attachments','_etag','_rid','_self','_ts','collectionType','id'}

    # Remove unwanted columns from the columns collection
    cols = list(set(df.columns) - unwanted_cols)

    profiles = df.select(cols)

    display(profiles.limit(10))
    ```

    これで出力には、希望する列のみが含まれるようになります。`preferredProducts` **(1)** と `productReviews` **2** 列に子要素が含まれていることがわかります。値を表示したい行で値を展開します。Azure Cosmos DB Data Explorer 内の `UserProfiles01` コンテナーで生の JSON 形式が表示されていたことを覚えているかもしれません。

    ![セルの'出力が表示されています。](media/cell2.png "Cell 2 output")

7. 取り扱っている記録の数を把握する必要があります。このためには、新しいセルで以下を入力して**実行**します。

    ```python
    profiles.count()
    ```

    100,000 というカウント結果が表示されるはずです。

8. 各ユーザーで `preferredProducts` 列の配列と `productReviews` 列の配列を使用して、レビューした製品を一致するお気に入りリストから製品のグラフを作成したいと考えています。これを行うには、この 2 列のフラット化された値が含まれている新しい DataFrame を 2 つ作成し、後ほど結合できるようにしなくてはなりません。新しいセルに以下を入力して**実行**します。

    ```python
    from pyspark.sql.functions import udf, explode

    preferredProductsFlat=profiles.select('userId',explode('preferredProducts').alias('productId'))
    productReviewsFlat=profiles.select('userId',explode('productReviews').alias('productReviews'))
    display(productReviewsFlat.limit(10))
    ```

    このセルでは、特別な PySpark [`explode` 関数](https://spark.apache.org/docs/latest/api/python/pyspark.sql.html?highlight=explode#pyspark.sql.functions.explode) をインポートしました。これは配列の各要素の新しい行を返します。この関数は、`preferredProducts` と `productReviews` 列をフラット化して、読みやすくしたりクエリを実行しやすくしたりする上で役立ちます。

    ![セルの出力。](media/cell4.png "Cell 4 output")

    `productReviewFlat` DataFrame の内容を表示するセルの出力を確認します。ユーザーのお気に入り製品リストに一致させたい `productId` と、表示または保存したい `reviewText` が含まれている新しい `productReviews` 列が表示されています。

9. `preferredProductsFlat` DataFrame の内容を見てみましょう。このためには、新しいセルで以下を入力して**実行**します。

    ```python
    display(preferredProductsFlat.limit(20))
    ```

    ![セルの出力。](media/cell5.png "Cell 5 results")

    お気に入り製品配列で `explode` 関数を使用したため、列の値がユーザーの順序で `userId` と `productId` 行にフラット化されました。

10. `productReviewFlat` DataFrame の内容をさらにフラット化して、`productReviews.productId` と `productReviews.reviewText` フィールドを抽出し、データの組み合わせごとに新しい行を作成する必要があります。このためには、新しいセルで以下を入力して**実行**します。

    ```python
    productReviews = (productReviewsFlat.select('userId','productReviews.productId','productReviews.reviewText')
        .orderBy('userId'))

    display(productReviews.limit(10))
    ```

    出力では、それぞれの `userId` に対して複数の行があることがわかります。

    ![セルの出力。](media/cell6.png "Cell 6 results")

11. 最後のステップは、`userId` と `productId` の値で `preferredProductsFlat` と `productReviews` DataFrames を結合し、お気に入り製品レビューのグラフを構築することです。このためには、新しいセルで以下を入力して**実行**します。

    ```python
    preferredProductReviews = (preferredProductsFlat.join(productReviews,
        (preferredProductsFlat.userId == productReviews.userId) &
        (preferredProductsFlat.productId == productReviews.productId))
    )

    display(preferredProductReviews.limit(100))
    ```

    > **注**: 「テーブル」 ビューの列ヘッダーをクリックして、結果セットを並べ替えてみてください。

    ![セルの出力。](media/cell7.png "Cell 7 results")

## 演習 4: Azure Synapse Analytics の サーバーレス SQL プールを使用して Azure Cosmos DB に対するクエリを実行する

Tailwind Traders は、T-SQL を使用して Azure Cosmos DB 分析ストアを探索したいと考えています。ビューを作成し、それを他の分析ストア コンテナーやデータ レイクからのファイルとの結合に使用し、Power BI のような外部ツールでアクセスできるようになれば理想的です。

### タスク 1: 新しい SQL スクリプトを作成する

1. 「**開発**」 ハブに移動します。

    ![開発ハブ](media/develop-hub.png "Develop hub")

2. **+** (1) を選択してから 「**SQL スクリプト**」 (2) を選択します。

    ![SQL スクリプト ボタンが強調表示されています。](media/new-script.png "SQL script")

3. スクリプトが開くと、右側に 「**プロパティ**」 ペインが表示されています **(1)**。「**名前**」 (2) に **`User Profile HTAP`** と入力し、「**プロパティ**」 ボタンを選択してペインを閉じます **(1)**。

    ![プロパティ ペインが表示されます。](media/new-script-properties.png "Properties")

4. サーバーレス SQL プール (**組み込み**) が選択されていることを確認します。

    ![サーバーレス SQL プールが選択されています。](media/built-in-htap.png "Built-in")

5. 次の SQL クエリを貼り付けます。OPENROWSET ステートメントで **`YOUR_ACCOUNT_NAME`** を Azure Cosmos DB アカウント名に置き換え、**`YOUR_ACCOUNT_KEY`** は コンテナー作成後に上記の手順 5 でコピーした Azure Cosmos DB 主キーの値に置き換えます。

    ```sql
    USE master
    GO

    IF DB_ID (N'Profiles') IS NULL
    BEGIN
        CREATE DATABASE Profiles;
    END
    GO

    USE Profiles
    GO

    DROP VIEW IF EXISTS UserProfileHTAP;
    GO

    CREATE VIEW UserProfileHTAP
    _AS
    SELECT
        *
    FROM OPENROWSET(
        'CosmosDB',
        N'account=YOUR_ACCOUNT_NAME;database=CustomerProfile;key=YOUR_ACCOUNT_KEY',
        UserProfileHTAP
    )
    WITH (
        userId bigint,
        cartId varchar(50),
        preferredProducts varchar(max),
        productReviews varchar(max)
    ) AS profiles
    CROSS APPLY OPENJSON (productReviews)
    WITH (
        productId bigint,
        reviewText varchar(1000)
    ) AS reviews
    GO
    ```

    完成したクエリは次のようになるはずです:

    ![クエリのビュー作成の部分と結果が表示されます。](media/htap-view.png "SQL query")

    クエリはまず、`Profiles` という名前の新しいサーバーレス SQL プール データベース (存在していない場合) を作成し、`USE Profiles` を実行し、`Profiles` データベースに対してスクリプトの残りの内容を実行します。次に、`UserProfileHTAP` ビューがある場合はこれをドロップします。最後に以下を実行します。

    - **1.**`UserProfileHTAP` という名前の SQL ビューを作成します。
    - **2.**`OPENROWSET` ステートメントを使用してデータ ソースのタイプを `CosmosDB` に設定し、アカウントの詳細を設定して、`UserProfileHTAP` という名前の Azure Cosmos DB 分析ストア コンテナーに対するビューを作成します。
    - **3.**`WITH` 句が JSON ドキュメントのプロパティ名に一致し、適切な SQL データ型を適用します。`preferredProducts` と `productReviews` フィールドが `varchar(max)` に設定されていることがわかります。これは、両方のプロパティに JSON 形式のデータが含まれているためです。
    - **4.**JSON ドキュメントの `productReviews` プロパティには入れ子になった部分配列が含まれているため、ドキュメントのプロパティと配列のあらゆる要素を「結合」します。Synapse SQL を使用すると、入れ子になった配列で `OPENJSON` 関数を適用して、入れ子になった構造をフラット化できます。Synapse ノートブックで先ほど Python `explode` 関数を使用した場合と同様に、`productReviews` 内で値をフラット化します。
    - **5.**出力には、ステートメントの実行が成功したことが示されます。

6. 「**データ**」 ハブに移動します。

    ![データ ハブ](media/data-hub.png "Data hub")

7. 「**ワークスペース**」 タブ **(1)** を選択し、データベース グループを展開します。**プロファイル** SQL オンデマンド データベース **(2)** を展開します。これがリストに表示されない場合は、データベース リストを更新します。「ビュー」 を展開し、**`UserProfileHTAP`** ビューを右クリックします **(3)**。「**新しい SQL スクリプト**」 (4) を選択してから 「**上位 100 行を選択**」 (5) を選びます。

    ![「上位 100 行を選択」 のクエリ オプションが強調表示されています。](media/new-select-query.png "New select query")

8. クエリを**実行**して結果を書き留めておきます。

    ![結果が表示されます。](media/select-htap-view.png "Select HTAP view")

    `preferredProducts` **(1)** と `productReviews` **(2)** フィールドがクエリに含まれており、両方に JSON 形式の値が含まれています。ビューの CROSS APPLY OPENJSON ステートメントが、`productId` と `reviewText` の値を新しいフィールドに抽出することで、入れ子になった部分配列を `productReviews` **(2)** フィールドでフラット化していることがわかります。
