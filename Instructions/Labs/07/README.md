# モジュール 7 - データ ウェアハウスにデータを取り込んで読み込む

このモジュールでは、T-SQL スクリプトと Synapse Analytics 統合パイプラインを介してデータ ウェアハウスにデータを取り込む方法を説明します。受講者は、T-SQL を使用して PolyBase と COPY で Synapse 専用 Synapse SQLプールにデータを読み込む方法を学びます。また、ペタバイト スケールのデータ インジェスト向けに Azure Synapse パイプラインでコピー アクティビティとともにワークロード管理を使用する方法も学習します。

このモジュールでは、次のことができるようになります。

- Azure Synapse パイプラインを使用してペタバイト規模のインジェストを行う
- T-SQL を使用して PolyBase と COPY でデータをインポートする
- Azure Synapse Analytics でデータ読み込みのベスト プラクティスを使用する

## ラボの詳細

- [モジュール 7 - データ ウェアハウスにデータを取り込んで読み込む](#module-7---ingest-and-load-data-into-the-data-warehouse)
  - [ラボの詳細](#lab-details)
  - [ラボの構成と前提条件](#lab-setup-and-pre-requisites)
  - [演習 0: 専用 SQL プールを起動する](#exercise-0-start-the-dedicated-sql-pool)
  - [演習 1: T-SQL を使用して PolyBase と COPY でデータをインポートする](#exercise-1-import-data-with-polybase-and-copy-using-t-sql)
    - [タスク 1: ステージング テーブルを作成する](#task-1-create-staging-tables)
    - [タスク 2: PolyBase 読み込み操作を設定して実行する](#task-2-configure-and-run-polybase-load-operation)
    - [タスク 3: COPY ステートメントを設定して実行する](#task-3-configure-and-run-the-copy-statement)
    - [タスク 4: クラスター化列ストア テーブルにデータを読み込む](#task-4-load-data-into-the-clustered-columnstore-table)
    - [タスク 5: COPY を使用して非標準行区切りでテキスト ファイルを読み込む](#task-5-use-copy-to-load-text-file-with-non-standard-row-delimiters)
    - [タスク 6: PolyBase を使用して非標準行区切りでテキスト ファイルを読み込む](#task-6-use-polybase-to-load-text-file-with-non-standard-row-delimiters)
  - [演習 2: Azure Synapse パイプラインを使用してペタバイト規模のインジェストを行う](#exercise-2-petabyte-scale-ingestion-with-azure-synapse-pipelines)
    - [タスク 1: ワークロード管理分類を構成する](#task-1-configure-workload-management-classification)
    - [タスク 2: コピー アクティビティを含むパイプラインを作成する](#task-2-create-pipeline-with-copy-activity)

## ラボの構成と前提条件

> **注:** ホストされたラボ環境を**使用しておらず**、ご自分の Azure サブスクリプションを使用している場合は、`Lab setup and pre-requisites` の手順のみを完了してください。その他の場合は、演習 0 にスキップします。

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

## 演習 0: 専用 SQL プールを起動する

このラボでは専用 SQL プールを使用します。最初の手順として、これが一時停止状態でないことを確認してください。一時停止している場合は、以下の手順に従って起動します。

1. Synapse Studio (<https://web.azuresynapse.net/>) を開きます。

2. 「**管理**」 ハブを選択します。

    ![管理ハブが強調表示されています。](media/manage-hub.png "Manage hub")

3. 左側のメニューで 「**SQL プール**」 を選択します **(1)**。専用 SQL プールが一時停止状態の場合は、プールの名前の上にマウスを動かして 「**再開**」  (2) を選択します。

    ![専用 SQL プールで再開ボタンが強調表示されています。](media/resume-dedicated-sql-pool.png "Resume")

4. プロンプトが表示されたら、「**再開**」 を選択します。プールが再開するまでに、1 ～ 2 分かかります。

    ![「再開」 ボタンが強調表示されています。](media/resume-dedicated-sql-pool-confirm.png "Resume")

> 専用 SQL プールが再開する間、**続行して次の演習に進みます**。

## 演習 1: T-SQL を使用して PolyBase と COPY でデータをインポートする

大量のデータを Azure Synapse Analytics に読み込むさまざまなオプションがあり、データ型も各種あります。Synapse SQL プールを使用した T-SQL コマンドや、Azure Synapse パイプラインの使用が挙げられます。このシナリオでは、Wide World Importers がほとんどの生データをさまざまな形式でデータ レイクに格納しています。WWI のデータ エンジニアは利用可能なデータ読み込みオプションのうち、T-SQL の使用に最も慣れています。

ただし、SQL によく慣れていても、大きなファイルやさまざまな種類と形式のファイルを読み込む際には考慮に入れるべきことがいくつかあります。ファイルは ADLS Gen2 に格納されているため、WWI は PolyBase 外部テーブルまたは新しい COPY ステートメントを使用できます。どちらのオプションでも、迅速でスケーラブルなデータ読み込みオプションが可能ですが、両者の合谷はいくつか異なる点があります。

| PolyBase | COPY |
| --- | --- |
| `CONTROL` 許可が必要 | 緩い許可 |
| 行の幅に制限がある | 行の幅に制限はない |
| テキスト内に区切り記号がない | テキストの区切り記号に対応 |
| ライン区切り記号は固定 | 列と行のカスタム区切り記号に対応 |
| コードでの設定が複雑 | コードの量は低減 |

WWI は、一般的に PolyBase の方が COPY よりも早いという情報を得ました。特に大きなデータ セットを使用する場合です。

この演習では、WWI がこれらの読み込み方法で設定の容易さや柔軟性、スピードを比べられるよう支援します。

### タスク 1: ステージング テーブルを作成する

`Sale` テーブルには、読み取り中心のワークロードで最適化できるよう列ストア インデックスが含まれています。また、クエリの報告やアドホック クエリでもよく使われます。読み込み速度を最速にして、`Sale` テーブルでの大量のデータ挿入の影響を最小限に抑えるため、WWI は読み込みのステージング テーブルを作成することにしました。

このタスクでは、`SaleHeap` という名前の新しいステージング テーブルを `wwi_staging` スキーマで作成します。これを[ヒープ](https://docs.microsoft.com/sql/relational-databases/indexes/heaps-tables-without-clustered-indexes?view=sql-server-ver15)として定義し、ラウンド ロビン分散を使用する必要があります。WWI はデータ読み込みパイプラインを最終的に決定すると、`SaleHeap` にデータを読み込んだ後、ヒープ テーブルから `Sale` に挿入する予定です。これは 2 段階のプロセスですが、行を運用テーブルに挿入する 2 番目の手順では、分散全体でデータの移動は発生しません。

また、`wwi_staging` 内で新しい `Sale` クラスター化列ストア テーブルを作成し、データ読み込み速度を比較します。

1. Synapse Analytics Studio (<https://web.azuresynapse.net/>) を開き、「**開発**」 ハブに移動します。

    ![開発メニュー項目が強調表示されています。](media/develop-hub.png "Develop hub")

2. 「**開発**」 メニューで + ボタンを選択し、コンテキストメニューから 「**SQL スクリプト**」 を選びます。

    ![「SQL スクリプト」 コンテキスト メニュー項目が強調表示されています。](media/synapse-studio-new-sql-script.png "New SQL script")

3. ツールバー メニューで、**SQL プール** データベースに接続してクエリを実行します。

    ![クエリ ツールバーの 「接続先」 オプションが強調表示されています。](media/synapse-studio-query-toolbar-connect.png "Query toolbar")

4. クエリ ウィンドウでスクリプトを以下に置き換えて、`wwi_staging` スキーマを作成します。

    ```sql
    CREATE SCHEMA [wwi_staging]
    ```

5. ツールバー メニューから 「**実行**」 を選択して、SQL コマンドを実行します。

    ![クエリ ツールバーの 「実行」 ボタンが強調表示されています。](media/synapse-studio-query-toolbar-run.png "Run")

    > **注:** 以下のエラーが表示されたら、次の手順に進みます: `Failed to execute query. Error: There is already an object named 'wwi_staging' in the database. CREATE SCHEMA failed due to previous errors.`

6. クエリ ウィンドウでスクリプトを以下に置き換えて、ヒープ テーブルを作成します。

    ```sql
    CREATE TABLE [wwi_staging].[SaleHeap]
    ( 
        [TransactionId] [uniqueidentifier]  NOT NULL,
        [CustomerId] [int]  NOT NULL,
        [ProductId] [smallint]  NOT NULL,
        [Quantity] [smallint]  NOT NULL,
        [Price] [decimal](9,2)  NOT NULL,
        [TotalAmount] [decimal](9,2)  NOT NULL,
        [TransactionDate] [int]  NOT NULL,
        [ProfitAmount] [decimal](9,2)  NOT NULL,
        [Hour] [tinyint]  NOT NULL,
        [Minute] [tinyint]  NOT NULL,
        [StoreId] [smallint]  NOT NULL
    )
    WITH
    (
        DISTRIBUTION = ROUND_ROBIN,
        HEAP
    )
    ```

7. ツールバー メニューから 「**実行**」 を選択して、SQL コマンドを実行します。

8. クエリ ウィンドウでスクリプトを以下に置き換え、読み込みの比較用に `wwi_staging` スキーマで `Sale` テーブルを作成します。

    ```sql
    CREATE TABLE [wwi_staging].[Sale]
    (
        [TransactionId] [uniqueidentifier]  NOT NULL,
        [CustomerId] [int]  NOT NULL,
        [ProductId] [smallint]  NOT NULL,
        [Quantity] [smallint]  NOT NULL,
        [Price] [decimal](9,2)  NOT NULL,
        [TotalAmount] [decimal](9,2)  NOT NULL,
        [TransactionDate] [int]  NOT NULL,
        [ProfitAmount] [decimal](9,2)  NOT NULL,
        [Hour] [tinyint]  NOT NULL,
        [Minute] [tinyint]  NOT NULL,
        [StoreId] [smallint]  NOT NULL
    )
    WITH
    (
        DISTRIBUTION = HASH ( [CustomerId] ),
        CLUSTERED COLUMNSTORE INDEX,
        PARTITION
        (
            [TransactionDate] RANGE RIGHT FOR VALUES (20100101, 20100201, 20100301, 20100401, 20100501, 20100601, 20100701, 20100801, 20100901, 20101001, 20101101, 20101201, 20110101, 20110201, 20110301, 20110401, 20110501, 20110601, 20110701, 20110801, 20110901, 20111001, 20111101, 20111201, 20120101, 20120201, 20120301, 20120401, 20120501, 20120601, 20120701, 20120801, 20120901, 20121001, 20121101, 20121201, 20130101, 20130201, 20130301, 20130401, 20130501, 20130601, 20130701, 20130801, 20130901, 20131001, 20131101, 20131201, 20140101, 20140201, 20140301, 20140401, 20140501, 20140601, 20140701, 20140801, 20140901, 20141001, 20141101, 20141201, 20150101, 20150201, 20150301, 20150401, 20150501, 20150601, 20150701, 20150801, 20150901, 20151001, 20151101, 20151201, 20160101, 20160201, 20160301, 20160401, 20160501, 20160601, 20160701, 20160801, 20160901, 20161001, 20161101, 20161201, 20170101, 20170201, 20170301, 20170401, 20170501, 20170601, 20170701, 20170801, 20170901, 20171001, 20171101, 20171201, 20180101, 20180201, 20180301, 20180401, 20180501, 20180601, 20180701, 20180801, 20180901, 20181001, 20181101, 20181201, 20190101, 20190201, 20190301, 20190401, 20190501, 20190601, 20190701, 20190801, 20190901, 20191001, 20191101, 20191201)
        )
    )
    ```

9. ツールバー メニューから 「**実行**」 を選択して、SQL コマンドを実行します。

### タスク 2: PolyBase 読み込み操作を設定して実行する

PolyBase では以下の要素が必要です。

- Parquet ファイルがある ADLS Gen2 で `abfss` パスを指す外部データ ソース
- Parquet ファイルの外部ファイル形式
- ファイルのスキーマのほか、場所、データ　ソース、ファイル形式を定義する外部テーブル

1. クエリ ウィンドウでスクリプトを以下に置き換えて、外部データ他のデータ ソースを作成します。必ず `SUFFIX` をラボのワークスペース ID に置き換えてください。

    ```sql
    -- Replace SUFFIX with the lab workspace id.
    CREATE EXTERNAL DATA SOURCE ABSS
    WITH
    ( TYPE = HADOOP,
        LOCATION = 'abfss://wwi-02@asadatalakeSUFFIX.dfs.core.windows.net'
    );
    ```

    ラボのワークスペース ID は Synapse Analytics ワークスペース名の最後、およびユーザー名に含まれています。

    ![サフィックスが強調表示されています。](media/data-lake-suffix.png "Data lake suffix")

2. ツールバー メニューから 「**実行**」 を選択して、SQL コマンドを実行します。

3. クエリ ウィンドウで、スクリプトを以下に置き換えて、外部ファイル形式と外部データ テーブルを作成します。`TransactionId` が `uniqueidentifier` ではなく、`nvarchar(36)` フィールドとして定義されている点に留意してください。外部テーブルは現在、`uniqueidentifier` 列をサポートしていないためです。

    ```sql
    CREATE EXTERNAL FILE FORMAT [ParquetFormat]
    WITH (
        FORMAT_TYPE = PARQUET,
        DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'
    )
    GO

    CREATE SCHEMA [wwi_external];
    GO

    CREATE EXTERNAL TABLE [wwi_external].Sales
        (
            [TransactionId] [nvarchar](36)  NOT NULL,
            [CustomerId] [int]  NOT NULL,
            [ProductId] [smallint]  NOT NULL,
            [Quantity] [smallint]  NOT NULL,
            [Price] [decimal](9,2)  NOT NULL,
            [TotalAmount] [decimal](9,2)  NOT NULL,
            [TransactionDate] [int]  NOT NULL,
            [ProfitAmount] [decimal](9,2)  NOT NULL,
            [Hour] [tinyint]  NOT NULL,
            [Minute] [tinyint]  NOT NULL,
            [StoreId] [smallint]  NOT NULL
        )
    WITH
        (
            LOCATION = '/sale-small%2FYear%3D2019',  
            DATA_SOURCE = ABSS,
            FILE_FORMAT = [ParquetFormat]  
        )  
    GO
    ```

    > **注:** `/sale-small/Year=2019/` フォルダーの Parquet ファイルには **339,507,246 行**が含まれています。

4. ツールバー メニューから 「**実行**」 を選択して、SQL コマンドを実行します。

5. クエリ ウィンドウで、スクリプトを以下に置き換えて、`wwi_staging.SalesHeap` テーブルにデータを読み込みます。このコマンドの所要時間は約 10 分です。

    ```sql
    INSERT INTO [wwi_staging].[SaleHeap]
    SELECT *
    FROM [wwi_external].[Sales]
    ```

    > これを実行している間に、ラボの手順の残りを読み、内容をよく理解しておいてください。

### タスク 3: COPY ステートメントを設定して実行する

それでは、COPY ステートメントを使用して同じ読み込み操作を行う方法を見てみましょう。

1. クエリ ウィンドウでスクリプトを以下に置き換え、COPY ステートメントを使用してヒープ テーブルを切り詰め、データを読み込みます。これを実行する前に、必ず `SUFFIX` をラボのワークスペース ID に置き換えてください。

    ```sql
    TRUNCATE TABLE wwi_staging.SaleHeap;
    GO

    -- Replace <PrimaryStorage> with the workspace default storage account name.
    COPY INTO wwi_staging.SaleHeap
    FROM 'https://asadatalakeSUFFIX.dfs.core.windows.net/wwi-02/sale-small%2FYear%3D2019'
    WITH (
        FILE_TYPE = 'PARQUET',
        COMPRESSION = 'SNAPPY'
    )
    GO
    ```

2. ツールバー メニューから 「**実行**」 を選択して、SQL コマンドを実行します。このコマンドの実行には数分かかります。このクエリの実行にかかった時間を**書き留めておきます**。

    > これを実行している間に、ラボの手順の残りを読み、内容をよく理解しておいてください。

3. クエリ ウィンドウで、スクリプトを以下に置き換えて、インポートされた行数を確認します。

    ```sql
    SELECT COUNT(1) FROM wwi_staging.SaleHeap(nolock)
    ```

4. ツールバー メニューから 「**実行**」 を選択して、SQL コマンドを実行します。`339507246` という結果が表示されます。

行数は両方の読み込み操作で一致しましたか? どのアクティビティがもっとも迅速でしたか? どちらもほぼ同じ時間で同じ量のデータをコピーしたことがわかります。

### タスク 4: クラスター化列ストア テーブルにデータを読み込む

上記の両方の読み込み操作では、データをヒープ テーブルに挿入しました。これをクラスター化列ストア テーブルに挿入すると、どうなるでしょう? パフォーマンスに大きな違いはあるのでしょうか? 調べてみましょう!

1. クエリ ウィンドウでスクリプトを以下に置き換え、COPY ステートメントを使用してクラスター化列ストア `Sale` テーブルにデータを読み込みます。必ず `SUFFIX` はワークスペースの ID に置き換えてください。このコマンドは**実行しないでください**。このコマンドは、実行に 7 分ほどかかるため、スキップします。

    ```sql
    -- Replace SUFFIX with the workspace default storage account name.
    COPY INTO wwi_staging.Sale
    FROM 'https://asadatalakeSUFFIX.dfs.core.windows.net/wwi-02/sale-small%2FYear%3D2019'
    WITH (
        FILE_TYPE = 'PARQUET',
        COMPRESSION = 'SNAPPY'
    )
    GO
    ```

結果はどうでしたか? 読み込み操作は、`Sale` テーブルとヒープ (`SaleHeap`) テーブルに書き込むよりも時間がかかｒましたか?

この場合、結果は次のようになります。

PolyBase vs. COPY (DW500) *(2019 年の小規模なデータ セット (339,507,246 行) を挿入)*:

- COPY (Heap:  **5:08**、クラスター化列ストア: **6:52**)
- PolyBase (Heap:  **5:59**)

### タスク 5: COPY を使用して非標準行区切りでテキスト ファイルを読み込む

PolyBase に対する COPY の利点のひとつは、列および行のカスタム区切り記号に対応する点です。

WWI では毎晩、パートナーの分析システムから地域の売上データを取り込み、データ レイクにファイルを保存しています。テキスト ファイルでは、列と行の非標準区切り記号を使用しており、列の区切りは `.`、行の区切りは `,` です: 

```text
20200421.114892.130282.159488.172105.196533,20200420.109934.108377.122039.101946.100712,20200419.253714.357583.452690.553447.653921
```

データには以下のフィールドが含まれています:  `Date`、`NorthAmerica`、`SouthAmerica`、`Europe`、`Africa`、`Asia`。このデータを処理して Synapse Analytics に格納する必要があります。

1. クエリ ウィンドウでスクリプトを以下に置換、`DailySalesCounts` テーブルを作成し、COPY ステートメントを使用してデータを読み込んでください。以前と同様、必ず `SUFFIX` はワークスペースの ID に置き換えてください:

    ```sql
    CREATE TABLE [wwi_staging].DailySalesCounts
        (
            [Date] [int]  NOT NULL,
            [NorthAmerica] [int]  NOT NULL,
            [SouthAmerica] [int]  NOT NULL,
            [Europe] [int]  NOT NULL,
            [Africa] [int]  NOT NULL,
            [Asia] [int]  NOT NULL
        )
    GO

    -- Replace <PrimaryStorage> with the workspace default storage account name.
    COPY INTO wwi_staging.DailySalesCounts
    FROM 'https://asadatalakeSUFFIX.dfs.core.windows.net/wwi-02/campaign-analytics/dailycounts.txt'
    WITH (
        FILE_TYPE = 'CSV',
        FIELDTERMINATOR='.',
        ROWTERMINATOR=','
    )
    GO
    ```

    `FIELDTERMINATOR` と `ROWTERMINATOR` のプロパティは、ファイルを適正に解析する上で役立つことがわかります。

2. ツールバー メニューから 「**実行**」 を選択して、SQL コマンドを実行します。

3. クエリ ウィンドウでスクリプトを以下に置き換えて、インポートされたデータを表示します。

    ```sql
    SELECT * FROM [wwi_staging].DailySalesCounts
    ORDER BY [Date] DESC
    ```

4. ツールバー メニューから 「**実行**」 を選択して、SQL コマンドを実行します。

5. グラフで結果を表示し、「**カテゴリ列**」 を `Date` に設定してみてください。

    ![結果はグラフで表示されます。](media/daily-sales-counts-chart.png "DailySalesCounts chart")

### タスク 6: PolyBase を使用して非標準行区切りでテキスト ファイルを読み込む

PolyBase を使用して同じ操作を試してみましょう。

1. クエリ ウィンドウでスクリプトを以下に置き換え、PolyBase を使用して新しい外部ファイル形式、外部テーブルを作成し、データを読み込みます。

    ```sql
    CREATE EXTERNAL FILE FORMAT csv_dailysales
    WITH (
        FORMAT_TYPE = DELIMITEDTEXT,
        FORMAT_OPTIONS (
            FIELD_TERMINATOR = '.',
            DATE_FORMAT = '',
            USE_TYPE_DEFAULT = False
        )
    );
    GO

    CREATE EXTERNAL TABLE [wwi_external].DailySalesCounts
        (
            [Date] [int]  NOT NULL,
            [NorthAmerica] [int]  NOT NULL,
            [SouthAmerica] [int]  NOT NULL,
            [Europe] [int]  NOT NULL,
            [Africa] [int]  NOT NULL,
            [Asia] [int]  NOT NULL
        )
    WITH
        (
            LOCATION = '/campaign-analytics/dailycounts.txt',  
            DATA_SOURCE = ABSS,
            FILE_FORMAT = csv_dailysales
        )  
    GO
    INSERT INTO [wwi_staging].[DailySalesCounts]
    SELECT *
    FROM [wwi_external].[DailySalesCounts]
    ```

2. ツールバー メニューから 「**実行**」 を選択して、SQL コマンドを実行します。

以下のようなエラーが表示されるはずです:  `クエリを実行できませんでした。エラー: HdfsBridge::recordReaderFillBuffer - Unexpected error encountered filling record reader buffer: HadoopExecutionException: Too many columns in the line.`.

なぜ難しいのでしょうか。[PolyBase ドキュメント](https://docs.microsoft.com/sql/t-sql/statements/create-external-file-format-transact-sql?view=sql-server-ver15#limitations-and-restrictions)を参照してください。

> 区切りテキスト ファイルの行区切り記号は、Hadoop の LineRecordReader でサポートされている必要があります。つまり、`\r`、`\n`、`\r\n` のいずれかでなくてはなりません。これらの区切り記号をユーザーが構成することはできません。

これは、PolyBase に比べて COPY の柔軟性が際立つ例です。

## 演習 2: Azure Synapse パイプラインを使用してペタバイト規模のインジェストを行う

Tailwind Traders は大量の売上データをデータ ウェアハウスに取り込む必要があります。反復可能なプロセスで、効率よくデータを読み込むことを希望しています。データを読み込む際には、データ移動ジョブを優先したいと考えています。

あなたは、大量の Parquet ファイルをインポートするための概念実証データ パイプラインを作成した後、読み込みのパフォーマンスを向上させるためのベスト プラクティスを作成することにしました。

データをデータ ウェアハウスに移送する際は頻繁に、ある程度の調整が必要で、ひとつまたは複数のデータ ソースからの移動をコーディネートしなくてはなりません。何らかの変換が必要なこともあります。変換手順はデータの移動中 (extract-transform-load - ETL) または移動後 (extract-load-transform - ELT) に実施できます。最新のデータ プラットフォームでは、抽出や解析、結合、標準化、強化、クレンジング、統合、フィルタリングといった一般的なデータ ラングリング操作すべてをシームレスに行う必要があります。Azure Synapse Analytics には、2 つの重要な機能のカテゴリが含まれています。データ フローとデータ調整 (パイプラインとして実装) です。

> ラボのこのセクションでは、調整に焦点を当てます。次のセクションでは、変換 (データ フロー) パイプラインに注目します。

### タスク 1: ワークロード管理分類を構成する

大量のデータを読み込む場合、最速のパフォーマンスにするため、一度にひとつの読み込みジョブのみを実行することがベストです。これが可能でない場合は、同時に実行する読み込みの数を最小限に抑えてください。大規模な読み込みジョブが予想される場合は、読み込み前に専用 SQL プールをスケール アップすることを検討してください。

パイプライン セッションには必ず十分なメモリを配分してください。これを実行するには、次のテーブルのインデックスを再構築するためのアクセス許可を持っているユーザーのリソース クラスを、推奨される最小値に変更します。

適切なコンピューティング リソースで読み込みを実行するには、読み込みを実行するように指定された読み込みユーザーを作成します。特定のリソース クラスまたはワークロード グループに各読み込みユーザーを割り当てます。読み込みを実行するには、いずれかの読み込みユーザーとしてサインインし、読み込みを実行します。読み込みは、ユーザーのリソース クラスで実行されます。

1. Synapse Studio (<https://web.azuresynapse.net/>) を開き、「**開発**」 ハブまでナビゲートします。

    ![開発メニュー項目が強調表示されています。](media/develop-hub.png "Develop hub")

2. 「**開発**」 メニューから **+** ボタン **(1)** を選択し、コンテキスト メニューから 「**SQL スクリプト**」 を選びます **(2)**。

    ![「SQL スクリプト」 コンテキスト メニュー項目が強調表示されています。](media/synapse-studio-new-sql-script.png "New SQL script")

3. ツールバー メニューで、**SQLPool01** データベースに接続してクエリを実行します。

    ![クエリ ツールバーの 「接続先」 オプションが強調表示されています。](media/synapse-studio-query-toolbar-connect.png "Query toolbar")

4. クエリ ウィンドウでスクリプトを以下に置き換え、ワークロード グループ `BigDataLoad` を作成します。これは、最低 50% のリソースを予約することでワークロードの分離を利用します (上限は 100%)。

    ```sql
    IF NOT EXISTS (SELECT * FROM sys.workload_management_workload_classifiers WHERE group_name = 'BigDataLoad')
    BEGIN
        CREATE WORKLOAD GROUP BigDataLoad WITH  
        (
            MIN_PERCENTAGE_RESOURCE = 50 -- integer value
            ,REQUEST_MIN_RESOURCE_GRANT_PERCENT = 25 --  (guaranteed a minimum of 4 concurrency)
            ,CAP_PERCENTAGE_RESOURCE = 100
        );
    END
    ```

5. ツールバー メニューから 「**実行**」 を選択して、SQL コマンドを実行します。

6. クエリ ウィンドウでスクリプトを以下に置き換え、新しいワークロード分類子 `HeavyLoader` を作成します。この分類子は、自分の環境内で作成した `asa.sql.import01` ユーザーを `BigDataLoad` ワークロード グループに割り当てます。最後に `sys.workload_management_workload_classifiers` から選択して、作成したばかりの分類子を含むあらゆる分類子を表示します。

    ```sql
    IF NOT EXISTS (SELECT * FROM sys.workload_management_workload_classifiers WHERE [name] = 'HeavyLoader')
    BEGIN
        CREATE WORKLOAD Classifier HeavyLoader WITH
        (
            Workload_Group ='BigDataLoad',
            MemberName='asa.sql.import01',
            IMPORTANCE = HIGH
        );
    END

    SELECT * FROM sys.workload_management_workload_classifiers
    ```

7. ツールバー メニューから 「**実行**」 を選択して、SQL コマンドを実行します。クエリ結果に新しい分類子が表示されます。

    ![新しいワークロード分類子が強調表示されています。](media/workload-classifiers-query-results.png "Workload Classifiers query results")

8. 「**管理**」 ハブに移動します。

    ![管理メニュー項目が強調表示されています。](media/manage-hub.png "Manage hub")

9. 左側のメニューで 「**リンク サービス**」 を選択し **(1)**、**`sqlpool01_import01` (2)** という名前のリンク サービスを選択します。

    ![リンク サービスが表示されます。](media/linked-services.png "Linked services")

10. 専用 SQL プール接続のユーザー名が、`HeavyLoader` 分類子に追加した **`asa.sql.import01` user**になっていることがわかります。新しいパイプラインでこのリンク サービスを使用して、データ読み込みアクティビティのリソースを予約します。

    ![ユーザー名が強調表示されています。](media/sqlpool01-import01-linked-service.png "Linked service")

11. 「**キャンセル**」 を選択してダイアログを閉じ、プロンプトが表示されたら 「**変更を破棄する**」 を選択します。

### タスク 2: コピー アクティビティを含むパイプラインを作成する

1. 「**統合**」 ハブに移動します。

    ![「統合ハブが強調表示されています。](media/integrate-hub.png "Integrate hub")

2. **+ (1)** を選択してから 「**パイプライン**」 (2) を選択して新しいパイプラインを作成します。

    ![新しいパイプライン コンテキスト メニュー項目が選択されています。](media/new-pipeline.png "New pipeline")

3. 新しいパイプラインの 「**プロパティ**」 ペインで、以下の**名前**を入力します:  **`Copy December Sales`**。

    ![名前プロパティが強調表示されています。](media/pipeline-copy-sales-name.png "Properties")

4. アクティビティ リスト内で 「**移動と変換**」 を展開し、「**データのコピー**」 アクティビティをパイプライン キャンバスにドラッグします。

    ![データのコピーをキャンバスにドラッグします。](media/pipeline-copy-sales-drag-copy-data.png "Pipeline canvas")

5. キャンバスで 「**データのコピー**」 アクティビティを選択し、「**全般**」 タブ **(1)** を選択して 「**名前**」 を **`Copy Sales` (2)** に設定します。

    ![全般タブで名前が強調表示されています。](media/pipeline-copy-sales-general.png "General tab")

6. 「**ソース**」 タブ **(1)** を選択した後、`Source dataset` の隣で 「**+ 新規**」 (2) を選択します。

    ![新規ボタンが強調表示されています。](media/pipeline-copy-sales-source-new.png "Source tab")

7. 「**Azure Data Lake Storage Gen2**」 データ ストア **(1)** を選択してから 「**続行**」 (2) を選択します。

    ![ADLS Gen2 が選択されています。](media/new-dataset-adlsgen2.png "New dataset")

8. 「**Parquet**」 形式 **(1)** を選び、「**続行**」 (2) を選択します。

    ![Parquet 形式が強調表示されています。](media/new-dataset-adlsgen2-parquet.png "Select format")

9. プロパティで名前を **asal400_december_sales (1)** に設定し、**asadatalakeNNNNNN** リンク サービス **(2)** を選択します。**`wwi-02/campaign-analytics/sale-20161230-snappy.parquet`** ファイルの場所 **(3)** を参照し、スキーマのインポート向けに 「**サンプル ファイルから**」 (4) を選択します。お使いになっているコンピューターに「このサンプル ファイルをダウンロード」(sale-small-20100102-snappy.parquet?raw=true)して、「**ファイルの選択**」 フィールド **(5)** でこれを参照します。「**OK**」を選択します (6)。

    ![プロパティが表示されます。](media/pipeline-copy-sales-source-dataset.png "Dataset properties")

    まったく同じスキーマが含まれていて、サイズがはるかに小さいサンプル Parquet ファイルをダウンロードしました。これは、コピーするファイルが、コピー アクティビティ ソース設定で自動的にスキーマを推論するには大きすぎるためです。

10. 「**シンク**」 タブ **(1)** を選択した後、`Sink dataset` の隣で 「**+ 新規**」 (2) を選択します。

    ![新規ボタンが強調表示されています。](media/pipeline-copy-sales-sink-new.png "Sink tab")

11. 「**Azure Synapse Analytics**」 データ ストア **(1)** を選択してから 「**続行**」 (2) を選択します。

    ![Azure Synapse Analytics が選択されています。](media/new-dataset-asa.png "New dataset")

12. プロパティで名前を **`asal400_saleheap_asa` (1)** に設定し、`asa.sql.import01` で Synapse Analytics に接続する **sqlpool01_import01** リンク サービス **(2)** を選択します。テーブル名については、テーブル名ドロップダウンをスクロールし、**wwi_perf.Sale_Heap** テーブル **(3)** を選択してから 「**OK**」 (4) を選択します。

    ![プロパティが表示されます。](media/pipeline-copy-sales-sink-dataset.png "Dataset properties")

13. 「**シンク**」 タブで 「**Copy コマンド**」 (1) のコピー メソッドを選択し、コピー前のスクリプトに以下を入力して、インポート前にテーブルをクリアします:  **`TRUNCATE TABLE wwi_perf.Sale_Heap` (2)**。

    ![説明された設定が表示されます。](media/pipeline-copy-sales-sink-settings.png "Sink")

    最速で最もスケーラブルなデータの読み込み方法は、PolyBase または COPY ステートメント **(1)** を使用することです。COPY ステートメントは、スループットの高いデータを SQL プールに取り込む最も柔軟性の高い方法です。

14. 「**マッピング**」 タブ **(1)** を選択してから 「**スキーマのインポート**」 (2) を選択肢、各ソースおよび宛先フィールドのマッピングを作成します。ソース列で **`TransactionDate`** **(3)** を選択し、`TransactionDateId` 宛先列にマッピングします。

    ![マッピングが表示されます。](media/pipeline-copy-sales-sink-mapping.png "Mapping")

15. 「**設定**」 タブ **(1)** を選択し、「**データ統合ユニット**」 を **`8` (2)** に設定します。ソース Parquet ファイルが大きいため、この操作が必要になります。

    ![データ統合ユニットの値が 8 に設定されています。](media/pipeline-copy-sales-settings.png "Settings")

16. 「**すべて公開**」 を選択した後、**公開**して新しいリソースを保存します。

    ![「すべて公開」 が強調表示されています。](media/publish-all-1.png "Publish all")

17. 「**トリガーの追加**」 (1) が選択してから、「**今すぐトリガー**」 (2) を選択します。パイプライン実行トリガーで 「**OK**」 を選択して開始します。

    ![今すぐトリガー](media/copy-pipeline-trigger-now.png "Trigger now")

18. 「**監視**」 ハブに移動します。

    ![監視ハブ メニュー項目が選択されています。](media/monitor-hub.png "Monitor hub")

19. 「**パイプラインの実行**」 (1) を選択します。パイプライン実行のステータスはここで確認できます **(2)**。ビューを更新する必要があるかもしれません **(3)**。パイプライン実行が完了したら、`wwi_perf.Sale_Heap` テーブルのクエリを行い、インポートされたデータを表示できます。

    ![パイプライン実行の完了が表示されています。](media/pipeline-copy-sales-pipeline-run.png "Pipeline runs")
