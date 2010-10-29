#!/bin/sh
#
# Copyright (C) 2007-2010 Rakuten, Inc.
#

CLUSTER_YML='../tools/cap_recipe/cluster.yml'
SEGMENTS_PER_NODE=2

pushd `dirname $0`

if [ ! -e testdata.txt ]; then
  echo "Creating test data ... "
  ./mkdat.rb
  ls -l testdata*.txt
  echo "done."
fi

if [ ! -e testdata.vf ]; then
  split_no=`ruby -ryaml -e 'puts YAML.load_file("'"$CLUSTER_YML"'")["nodes"].size * '"$SEGMENTS_PER_NODE"`

  echo "Creating VFiles ... "
  for DATA in testdata testdata_multi testdata_join_a testdata_join_b
  do
    echo "  [$DATA]"
    fairy cp --split-no="$split_no" "$DATA".txt "$DATA".vf
  done
  echo "done."
fi

date > run_all.log

ls -1 fairy*_spec.rb | while read SPEC
do
  echo "----- $SPEC -----"
  rspec -fs "$SPEC" | (tee -a run_all.log) 2>&1
  echo ""
  sleep 3
done

date >> run_all.log

echo "Removing temporary files ... "

ls -1 /tmp/fairy_spec_*.txt | while read TMP_TXT
do
  echo "  [$TMP_TXT]"
  rm -f "$TMP_TXT"
done

ls -1 /tmp/fairy_spec_*.vf | while read TMP_VF
do
  echo "  [$TMP_VF]"
  fairy rm "$TMP_VF"
done

echo "done."

echo "===== SUMMARY ====="
echo "Fairy"
sed -ne '/^  should/p' run_all.log
echo ""

popd


