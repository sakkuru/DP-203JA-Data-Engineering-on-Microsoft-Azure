---
lab:
  title: Azure Synapse Link を使用して Hybrid Transactional Analytical Processing (HTAP)をサポートする
  module: Module 9
---

# <a name="lab-9---support-hybrid-transactional-analytical-processing-htap-with-azure-synapse-link"></a>ラボ 9 - Azure Synapse Link を使用したハイブリッド トランザクション分析処理 (HTAP) のサポート

このラボでは、受講者はAzure Synapse Link によって Azure Cosmos DB アカウントを Synapse ワークスペースにシームレスに接続する方法を学びます。 Synapse Link を有効にして構成する方法、および Apache Spark プールと SQL サーバーレス プールを使用して Azure Cosmos DB 分析ストアのクエリを行う方法を学びます。

このラボを完了すると、次のことができるようになります。

- Azure Cosmos DB を使用して Azure Synapse Link を構成する
- Synapse Analytics用のApache Sparkを使用してAzure Cosmos DBをクエリ
- Azure Synapse AnalyticsのサーバーレスSQLプールを使用してAzure Cosmos　DBにクエリを実行

## <a name="lab-setup-and-pre-requisites"></a>ラボの構成と前提条件

このラボを開始する前に、**ラボ 6: *Azure Data Factory または Azure Synapse Pipelines を使用したデータの変換*** を完了する必要があります。

> **注**:ラボ 6 を完了して "***いない***" が、このコースのラボのセットアップを<u>完了</u>している場合は、これらの手順を完了して、必要なリンク サービスとデータセットを作成できます。
>
> 1. Synapse Studio の **[管理]** ハブで、次の設定を使用して **Azure Cosmos DB (SQL API)** 用の新しい**リンク サービス**を追加します。
>       - **名前**: asacosmosdb01
>       - **Cosmos DB アカウント名**: asacosmosdb*xxxxxxx*
>       - **データベース名**: CustomerProfile
> 2. **[データ]** ハブで、次の**統合データセット**を作成します。
>       - **ソース**:Azure Cosmos DB (SQL API)
>       - **名前**: asal400_customerprofile_cosmosdb
>       - **リンク サービス**: asacosmosdb01
>       - **コレクション**:OnlineUserProfile01
>       - **スキーマのインポート**:接続/ストアから

## <a name="exercise-1---configuring-azure-synapse-link-with-azure-cosmos-db"></a>演習 1 – Azure Cosmos DB を使用して Azure Synapse Link を構成する

Tailwind Traders では Azure Cosmos DB を使用して、電子商取引サイトからユーザー プロファイル データを格納しています。 Azure Cosmos DB SQL API によって提供される NoSQL ドキュメント ストアでは、使い慣れた SQL 構文を使用してデータを管理する一方で、ファイルの読み取りと書き込みは大規模なグローバル スケールで行うことができます。

Tailwind Traders では Azure Cosmos DB の機能やパフォーマンスに満足していますが、データ ウェアハウスから複数のパーティションにわたる大量の分析クエリ (クロスパーティション クエリ) を実行するコストについて懸念しています。 Azure Cosmos DB の要求ユニット (RU) を増やすことなく、すべてのデータに効率的にアクセスする必要があります。 Azure Cosmos DB 変更フィード メカニズムを使用して、変更時にコンテナーからデータ レイクにデータを抽出するためのオプションに注目しました。 このアプローチの問題は、追加のサービスとコードの依存関係、およびソリューションの長期的なメンテナンスです。 Synapse パイプラインから一括エクスポートを実行できますが、任意の時点での最新情報は得られません。

Cosmos DB 用の Azure Synapse Link を有効にし、Azure Cosmos DB コンテナー上で分析ストアを有効にすることに決めます。 この構成では、すべてのトランザクション データが完全に分離された列ストアに自動的に格納されます。 このストアを使用すると、トランザクション ワークロードに影響を与えたり、リソース ユニット (RU) コストを発生させたりすることなく、Azure Cosmos DB 内のオペレーショナル データに対して大規模な分析を実行できます。 Cosmos DB 用の Azure Synapse Link では、Azure Cosmos DB と Azure Synapse Analytics 間に緊密な統合が作成されます。これにより、Tailwind Traders は、ETL なしでのオペレーショナル データに対するほぼリアルタイムの分析と、トランザクション ワークロードからの完全なパフォーマンスの分離を実行できます。

Azure Synapse Link では、Cosmos DB のトランザクション処理の分散スケールと、Azure Synapse Analytics の組み込みの分析ストアおよびコンピューティング能力を組み合わせることにより、Tailwind Trader のビジネス プロセスを最適化するためのハイブリッド トランザクション/分析処理 (HTAP) アーキテクチャが有効になります。 この統合により、ETL プロセスが不要になり、ビジネス アナリスト、データ エンジニア、データ サイエンティストがセルフサービスを実現し、ほぼリアルタイムの BI、分析、および Machine Learning パイプラインをオペレーショナル データに対して実行できるようになります。

### <a name="task-1-enable-azure-synapse-link"></a>タスク 1:Azure Synapse Link を有効にする

1. Azure portal (<https://portal.azure.com>) で、お使いのラボ環境のリソース グループを開きます。

2. **[Azure Cosmos DB アカウント]** を選択します。

    ![Azure Cosmos DB アカウントが強調表示されています。](images/resource-group-cosmos.png "Azure Cosmos DB アカウント")

3. 左側のメニューで **[Azure Synapse Link]** を選択します。
   a. **[アカウントが有効]** が設定されていることを確認します。\
   b. **[コンテナーで有効な Azure Synapse Link]** で、 *[cosmos_db_htap]* と *[OnlineUserProfile01]* が選択されていることを確認します。

  
4. **[有効化]** を選択します。

    ![[有効にする] が強調表示されます。](images/synapse-link-enable.png "Azure Synapse Link")

    Azure Cosmos DB コンテナーで分析ストアを有効にする前に、まず Azure Synapse Link を有効にする必要があります。

5. 続行する前に、この操作が完了するのを待ってください。1 分ほどかかります。 Azure **通知**アイコンを選択して、ステータスをチェックします。

    ![[Synapse Link の有効化] のプロセスが実行中です。](images/notifications-running.png "通知")

    完了すると、[Synapse リンクの有効化] の横に緑色のチェックマークが表示されます。

    ![操作は正常に完了しました。](images/notifications-completed.png "通知")

## <a name="exercise-2---querying-azure-cosmos-db-with-apache-spark-for-synapse-analytics"></a>演習 2 - Synapse Analytics で Apache Spark を使用して Azure Cosmos DB に対してクエリを実行する

Tailwind Traders は Apache Spark を使用し、新しい Azure Cosmos DB コンテナーに対して分析クエリを実行したいと考えています。 このセグメントでは、Synapse Studio の組み込みジェスチャを使用して、トランザクション ストアに影響を与えることなく、Synapse ノートブックをすばやく作成し、HTAP が有効になったコンテナーの分析ストアからデータを読み込みます。

Tailwind Traders は各ユーザーで特定されたお気に入りの製品リストを、レビュー履歴で一致する製品 ID に組み合わせて利用し、すべてのお気に入り製品レビューのリストを表示しようとしています。

### <a name="task-1-create-a-notebook"></a>タスク 1:ノートブックを作成する

1. **[データ]** ハブに移動します。

    ![[データ] ハブ。](images/data-hub.png "データ ハブ")

2. **[リンク]** タブを選択し、**[Azure Cosmos DB]** セクションを展開し (表示されない場合は、右上の **[&#8635;]** ボタンを使用して Synapse Studio を更新します)、**asacosmosdb01 (CustomerProfile)** リンク サービスを展開します。 **[OnlineUserProfile01]** コンテナーを右クリックし、 **[新しいノートブック]** を選択して、 **[DataFrame に読み込む]** を選択します。


    作成した **[OnlineUserProfile01]** コンテナーのアイコンは、他のコンテナーのアイコンとわずかに異なっていることがわかります。 これは、分析ストアが有効になっていることを示します。

3. 新しいノートブックで、**[アタッチ先]** ドロップダウン リストから **SparkPool01** Spark プールを選択します。

    ![[アタッチ先] ドロップダウン リストが強調表示されています。](images/notebook-attach.png "Spark プールのアタッチ")

4. **[すべて実行]** を選択します。


    初めて Spark セッションを開始する際は数分かかります。

    セル 1 内で生成されたコードでは、**spark.read** 形式が **cosmos.olap** に設定されていることがわかります。 これにより、Synapse リンクはコンテナー分析ストアを使用するよう指示されます。 トランザクション ストアに接続したい場合は (変更フィードから読み取ったり、コンテナーに書き込んだりするなど)、**cosmos.oltp** を使用します。

    > **注:**  分析ストアに書き込むことはできず、読み取りのみが可能です。 コンテナーにデータを読み込みたい場合は、トランザクション ストアに接続する必要があります。

    最初のオプションは Azure Cosmos DB リンク サービスの名前を構成します。 2 番目の `option` は、読み取り元の Azure Cosmos DB コンテナーを定義します。

5. 実行したセルの下にある **[+ コード]** ボタンを選択します。 これにより、最初のコード セルの下に新しいコード セルが追加されます。

6. DataFrame には不要な追加の列が含まれています。 不要な列を削除し、DataFrame のクリーンなバージョンを作成してみましょう。 そのためには、新しいコード セルに次のように入力して実行します。

    ```python
    unwanted_cols = {'_attachments','_etag','_rid','_self','_ts','collectionType','id'}

    # Remove unwanted columns from the columns collection
    cols = list(set(df.columns) - unwanted_cols)

    profiles = df.select(cols)

    display(profiles.limit(10))
    ```

    これで出力には、希望する列のみが含まれるようになります。 **preferredProducts** と **productReviews** 列に子要素が含まれていることがわかります。 値を表示したい行で値を展開します。 Azure Cosmos DB Data Explorer 内の **UserProfiles01** コンテナーで生の JSON 形式が表示されていたことを覚えているかもしれません。

    ![セルの出力が表示されています。](images/cell2.png "セル 2 の出力")

7. 取り扱っている記録の数を把握する必要があります。 そのためには、新しいコード セルに次のように入力して実行します。

    ```python
    profiles.count()
    ```

    99,999 というカウント結果が表示されるはずです。

8. 各ユーザーで **preferredProducts** 列の配列と **productReviews** 列の配列を使用して、レビューした製品を一致するお気に入りリストから製品のグラフを作成したいと考えています。 これを行うには、この 2 列のフラット化された値が含まれている新しい DataFrame を 2 つ作成し、後ほど結合できるようにしなくてはなりません。 新しいコード セルに以下を入力して実行します。

    ```python
    from pyspark.sql.functions import udf, explode

    preferredProductsFlat=profiles.select('userId',explode('preferredProducts').alias('productId'))
    productReviewsFlat=profiles.select('userId',explode('productReviews').alias('productReviews'))
    display(productReviewsFlat.limit(10))
    ```

    このセルでは、特別な PySpark [explode 関数](https://spark.apache.org/docs/latest/api/python/pyspark.sql.html?highlight=explode#pyspark.sql.functions.explode) をインポートしました。これは配列の各要素の新しい行を返します。 この関数は、**preferredProducts** と **productReviews** 列をフラット化して、読みやすくしたりクエリを実行しやすくしたりする上で役立ちます。

    ![セルの出力です。](images/cell4.png "セル 4 の出力")

    **productReviewFlat** DataFrame の内容を表示するセルの出力を確認します。 ユーザーのお気に入り製品リストに一致させたい **productId** と、表示または保存したい **reviewText** が含まれている新しい **productReviews** 列が表示されています。

9. **preferredProductsFlat** DataFrame の内容を見てみましょう。 このためには、新しいセルで以下を入力して**実行**します。

    ```python
    display(preferredProductsFlat.limit(20))
    ```

    ![セルの出力です。](images/cell5.png "セル 5 の結果")

    お気に入り製品配列で **explode** 関数を使用したため、列の値がユーザーの順序で **userId** と **productId** 行にフラット化されました。

10. **productReviewFlat** DataFrame の内容をさらにフラット化して、**productReviews.productId** と **productReviews.reviewText** フィールドを抽出し、データの組み合わせごとに新しい行を作成する必要があります。 そのためには、新しいコード セルに次のように入力して実行します。

    ```python
    productReviews = (productReviewsFlat.select('userId','productReviews.productId','productReviews.reviewText')
        .orderBy('userId'))

    display(productReviews.limit(10))
    ```

    出力には、それぞれの `userId` に対して複数の行があることがわかります。

    ![セルの出力です。](images/cell6.png "セル 6 の結果")

11. 最後のステップは、**userId** と **productId** の値で **preferredProductsFlat** と **productReviews** DataFrames を結合し、お気に入り製品レビューのグラフを構築することです。 そのためには、新しいコード セルに次のように入力して実行します。

    ```python
    preferredProductReviews = (preferredProductsFlat.join(productReviews,
        (preferredProductsFlat.userId == productReviews.userId) &
        (preferredProductsFlat.productId == productReviews.productId))
    )

    display(preferredProductReviews.limit(100))
    ```

    > **注**: [テーブル] ビューの列ヘッダーをクリックして、結果セットを並べ替えることができます。

    ![セルの出力です。](images/cell7.png "セル 7 の結果")

12. ノートブックの右上にある **[セッションの停止]** ボタンを使用して、ノートブック セッションを停止します。 次に、ノートブックを閉じて、変更を破棄します。

## <a name="exercise-3---querying-azure-cosmos-db-with-serverless-sql-pool-for-azure-synapse-analytics"></a>演習 3 - Azure Synapse Analytics の サーバーレス SQL プールを使用して Azure Cosmos DB に対するクエリを実行する

Tailwind Traders は、T-SQL を使用して Azure Cosmos DB 分析ストアを探索したいと考えています。 ビューを作成し、それを他の分析ストア コンテナーやデータ レイクからのファイルとの結合に使用し、Power BI のような外部ツールでアクセスできるようになれば理想的です。

### <a name="task-1-create-a-new-sql-script"></a>タスク 1:新しい SQL スクリプトを作成する

1. **[開発]** ハブに移動します。

    ![[開発] ハブ。](images/develop-hub.png "[開発] ハブ")

2. **+** メニューで、 **[SQL スクリプト]** を選択します。

    ![SQL スクリプト ボタンが強調表示されています。](images/new-script.png "SQL スクリプト")

3. スクリプトが開いたら、右側の **[プロパティ]** ペインで、 **[名前]** を `User Profile HTAP` に変更します。 次に、**[プロパティ]** ボタンを使用して、ペインを閉じます。

    ![プロパティ ペインが表示されています。](images/new-script-properties.png "プロパティ")

4. サーバーレス SQL プール (**組み込み**) が選択されていることを確認します。

    ![サーバーレス SQL プールが選択されています。](images/built-in-htap.png "組み込み")

5. 次の SQL クエリを貼り付けます。 OPENROWSET ステートメントで **YOUR_ACCOUNT_NAME** を Azure Cosmos DB アカウント名に置き換え、**YOUR_ACCOUNT_KEY** を Azure portal の **[キー]** ページにある Azure Cosmos DB 主キー に置き換えます (別のタブで開いたままにする必要があります)。

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

    DROP VIEW IF EXISTS OnlineUserProfile01;
    GO

    CREATE VIEW OnlineUserProfile01
    AS
    SELECT
        *
    FROM OPENROWSET(
        'CosmosDB',
        N'account=YOUR_ACCOUNT_NAME;database=CustomerProfile;key=YOUR_ACCOUNT_KEY',
        OnlineUserProfile01
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

6. **[実行]** ボタンを使用して、クエリを実行します。これにより、以下が行われます。
    - 存在しない場合は、**Profiles** という名前の新しいサーバーレス SQL プール データベースを作成します
    - データベース コンテキストを **Profiles** データベースに変更します。
    - **[OnlineUserProfile01]** ビューが存在する場合は、ドロップします。
    - **OnlineUserProfile01** という名前の SQL ビューを作成します。
    - OPENROWSET ステートメントを使用してデータ ソースのタイプを **CosmosDB** に設定し、アカウントの詳細を設定して、**OnlineUserProfile01** という名前の Azure Cosmos DB 分析ストア コンテナーに対するビューを作成します。
    - JSON ドキュメントのプロパティ名に一致し、適切な SQL データ型を適用します。 **preferredProducts** と **productReviews** フィールドが **varchar(max)** に設定されていることがわかります。 これは、両方のプロパティに JSON 形式のデータが含まれているためです。
    - JSON ドキュメントの **productReviews** プロパティには、入れ子になった部分配列が含まれているため、スクリプトは、ドキュメントのプロパティと配列のあらゆる要素を "結合" する必要があります。 Synapse SQL を使用すると、入れ子になった配列で OPENJSON 関数を適用して、入れ子になった構造をフラット化できます。 Synapse ノートブックで先ほど Python **explode** 関数を使用した場合と同様に、**productReviews** 内で値をフラット化します。

7. **[データ]** ハブに移動します。

    ![[データ] ハブ。](images/data-hub.png "データ ハブ")

8. **[ワークスペース]** タブを選択し、 **[SQL データベース]** グループを展開します。 **Profiles** SQL オンデマンド データベースを展開します (リストにこれが表示されない場合は、**データベース** リストを更新します)。 **[ビュー]** を展開し、 **[OnlineUserProfile01]** ビューを右クリックし、 **[新しい SQL スクリプト]** を選択してから、 **[上位 100 行を選択]** を選びます。

    ![[上位 100 行を選択] クエリ オプションが強調表示されています。](images/new-select-query.png "新しい選択クエリ")

9. スクリプトが**組み込み** SQL プールに接続されていることを確認してから、クエリを実行して結果を表示します。

    ![ビューの結果が表示されています。](images/select-htap-view.png "HTAP ビューを選択する")

    **preferredProducts** と **productReviews** フィールドがビューに含まれており、両方に JSON 形式の値が含まれています。 ビューの CROSS APPLY OPENJSON ステートメントが、**productId** と **reviewText** の値を新しいフィールドに抽出することで、入れ子になった部分配列を **productReviews** フィールドでフラット化していることがわかります。
