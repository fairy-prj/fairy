fairy programming interface

楽天株式会社        楽天技術研究所                

== プログラミングインターフェース
=== フィルタ系メソッド

--- Filter#map(block_source)
    ((|block_source|)): ブロックソースコード.

    Rubyの Enumerable#map, Enumerable#collect 相当.
    ブロックに対し、要素を入力とし、評価した結果を返す

--- Filter#collect

    Filter#map の alias.

--- Filter#select(block_source)
    ((|block_source|))ブロックソースコード. 

    Rubyの Enumerable#select 相当. 
    ブロックに対し、要素を入力とし、評価した結果が真の時、その要素を返す

--- Filter#grep(regexp)
    ((|regexp|)) 正規表現.

    Rubyの Enumerable#grep 相当.
    入力要素が((|regexp|))でマッチしたら、その要素を返す.

--- Filter#map_flatten(block_source)
    ((|block_source|))ブロックソースコード

    Rubyの Enumerable#map_flatten 相当.

    mapの結果をflattenして返す.

--- Filter#mapf
    Filter#map_flattenのalias

    例:

    filter.mapf(%{|line| line.split(/\s+/)}...

--- Filter#inject(block_source, :init_value=>...)

    結合法則の保証はユーザにまかされる. 
    init_value は 各inject で適用される(単位元になっている必要がある).

--- InjectFilter#value

    値の取り出し.

--- Filter#cat(filter_1, filter_2,…)

    複数のストリームの結合したストリームを返す.


--- Filter#equijoin(other_filter, main_column_no, [other_column_no])
    関係演算のjoin.

    ((|filter|))の((|main_column_no|))番目のカラムと、((|other_filter
    のother_column_no|))番目の等結合を返す.

--- Filter#direct_product(filter_1, filter_2,…, block_source)

    ((|block_source|))ブロックソースコード. ブロックソースへのブロック
    引数は(({|e0, e1, …|})).

    Rubyの Array#product 相当. 

--- Filter#barrier(:mode => mode, :cond => condition_block, :buffer => opt)

    ストリームを同期させるフィルタ. 

    ((|mode|))

    ((|BARRIER_NODE_CREATION|))条件が成立するまでノード作成させない.

    ((|BARRIER_STREAM|))データを流さない.

    ((|cond|))

    ブロックソース. ブロックを評価し, その条件が成立するまでブロック

    ((|NODE_CREATION|)) 前段のノードがすべてそろうまでブロック

    ((|DATA_ARRIVED|)) 全サブストリームからデータが来はじめるまでブロック

    ((|ALL_DATA|)) 全データが出力されるまでブロック

    ((|buffer|))

    ((|MEMORY|))メモリにためる

    ((|FILE|))  一時ファイルにためる

--- Filter#product

    direct_productのalias

--- Filter#*(other_filter)

    Filter#direct_product(other_filer)と同じ

--- Filter.group_by(block_source)

    入力された要素を((|block_source|))で評価し、それによりグルーピングし、後
    段へは、グループされた要素のストリームを渡す。
    Rubyのgroup_byと似ている.

    例: ワードカウント

	finput = fairy.input(input.vf)
	fmap = finput.mapf(%{|l| l.chomp.split})
	fshuffle = fmap.group_by(%{|w| w})
	freduce = fshuffle.map(%q{|values| "#{values.key}\t#{values.size}"})
	for w in freduce.here
	  puts w
	end

--- Filter#basic_group_by(block_source)

    group_by一族で, 一番基本的なもの.
    グループがそのままセグメントになる.

--- Filter.basic_mgroup_by(block_source)

    ((|basic_group_by|))の複数キー版 
    複数のキー毎にグルーピングされる.
    グループがそのままセグメント単位になる.

--- Filter#merge_group_by(block_source)

    ((|basic_group_by|))とは違い、後段には、上流のセグメント単位にグルー
    ピングしたストリームを要素として渡す.

--- Filter#seg_zip（filter_1, ..., filter_n, block_source)

    複数のストリームのセグメント毎に要素を順番に付き合わせる.

--- Filter#seg_join(filter_1,…, block_source)
    ((|block_source|))ブロックソースコード.

    ((|block_source|))へのブロック引数は |in0, in1, … in_n, out|
    セグメント単位にjoin処理を行う.

    オプション引数　:by -> :key を指定すると
    keyごとの突合せになる.

--- Filter#seg_shuffle(block_source)

    ((|block_source|)) ブロックソース. ブロックソースへのブロック引数
    は、入力セグメント列、出力セグメント列 (({|in_segment_stream、
    out_segment_stream|}))
    セグメント列をblock_source順に並べ替える

    ((|cond|))
	+ ブロックソース. ブロックを評価し, その条件が成立するまでブロック
	((|NODE_CREATION|)) 前段のノードがすべてそろうまでブロック
	((|DATA_ARRIVED|)) 全サブストリームからデータが来はじめるまでブロック
	((|ALL_DATA|)) 全データが出力されるまでブロック
    ((|buffer|))
	((|MEMORY|))メモリにためる
	((|FILE|))  一時ファイルにためる


=== 入力系メソッド

--- Fairy#input(desc)

    ((|desc|)) ローカルファイルや仮想ファイルの入力を指定する.

=== 定義系メソッド

--- Fairy::def_filter(name, [:sub=>true]){|input, optionparam…| …}

    ユーザレベルでフィルタを定義する
    ((|sub|))が真だとサブルーチン化する

=== フィルタ制御
--- Filter#sub(%{|input, subfairy| input.filer...})

    ある程度まとまった処理があったとして、その処理が終わったら後片付け
    したいとき用いる.
    新しい、fairyとcontrollerを立て、親とは別に処理を行う.

--- BEGIN/END

    filter.filter(..., :BEGIN=>begin_block_source, :END=>end_block_source)

    ((|begin_block_source|))各フィルターで最初に実行

    ((|end_block_source|)) 最後に実行

--- break
    フィルタリング処理をbreakする

--- fairy#def_pool_variable(var, value)
    fairy#def_pool_variable(var, :block => %{...})

    ((|var|))プール変数名.

    各フィルタで共有できるプール変数を定義する.
    ブロックソースが渡された場合は, コントローラ側でブロックを評価し, 
    その値を代入する.
    ((|value|))はディープコピーされる.

--- fairy#pool_variable[:var] = value

    プール変数に代入する.

    ブロックソース内からアクセスする方法:

	...map(#{|e| @Pool.var =  ...})

--- job インスタンス変数

    ...map(#{|e| ＠var ...})

    複数イテレーションで共有できる変数. ただし, 各セグメント内ローカルになる.


--- VArray
    仮想配列.

    各ストリームの計算結果を仮想的な配列に保存する.
    イテレーションするときに再利用できる.
    複数の下位ストリームにデータを流すことが可能.

    利用はinput/outoutで指定する. 

    配列としてのアクセスも用意されているが、 fairy的な動きはしない.

    Fairy.input(varray)
	varrayを入力として指定する.
    filter.output(VArray)
        VArray に 出力する.
    job.to_va
        VArrayに出力し、それを返す.

    例:
	va = fairy.input(Fairy::Iota, 1000).to_va
	10.times do {|i| va = fairy.input(va).map(%{|i| i*2}).to_va} 



=== 例外

    ブロック実行中に発生した例外を通知する

===  STDOUT

    ブロック内で puts 等を実行すると、クライアント側に出力される。

    putsは$stdoutを参照し、かつ、グローバル変数なので、スレッドセーフ
    にするために、ちょっといやらしいことをした。


=== ログAPI

--- Fairy::log(sender, printfメッセージ, param...)
    Fairy::log(printfメッセージ, param...)
    Fairy::log(sender) {|sio| ...}

--- Fairy::fatal, error, nortify, info, debug(.logと同じパラメータ)

    普段はこちらを使う。出力レベルに応じて出力する

--- Fairy::log_exception(sender, exp), Fairy::log_exception(exp)
    例外のバックトレースをログに書き出す

    例:

    Log::warn(self) do |sio|
      sio.puts "Warn: Exception raised:"
      sio.puts $!
      for l in $@
        sio.puts "\t#{l}"
      end
    end

--- fairy.conf

    fairyの環境設定を行うファイル.

    検索パス(存在したら上書き).

    /etc/fairy.conf , $FAIRY_CONF, $HOME/.fiaryrc, ./etc/fairy.conf

    グローバルな設定
	CONF.MASTER_HOST
	CONF.MASTER_PORT
	CONF.HOME
	CONF.VF_ROOT

    ホスト固有の設定
        CONF[“hostname”].MASTER_HOST
        VArray に 出力する


