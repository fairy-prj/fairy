# -*- coding: utf-8 -*-
# encoding: UTF-8
#
# Copyright (C) 2007-2010 Rakuten, Inc.
#

require "e2mmap"

module Fairy
  extend Exception2MessageMapper

  class BreakCreateNode<Exception;end
  class AbortCreateNode<Exception;end

  module ERR
    extend Exception2MessageMapper

    def_exception :CantAcceptBlock,
      "ブロックは受け付けられません" 
    def_exception :NodeNotArrived, 
      "%s のホスト上でnodeが立ち上がっていません"
    def_exception :NoExistProcessorWithObject, 
      "%s の存在するプロセッサは存在しません"
    def_exception :NoSuchMode, 
      "そのモードはありません(%s)"
    
    def_exception :UnrecognizedOption,
      "そのオプションは分かりません(%s)"

    def_exception :AlreadyAssignedVarriable,
      "すでに変数(%s)は登録されています"
    def_exception :NoAssignedVarriable,
      "すでに変数(%s)は登録されていません"

    def_exception :NoSupportClass,
      "そのクラスはサポートしていません(%s)"

    def_exception :NoVFile,
      "VFileではありません(%s)"
    def_exception :IllegalVFile,
      "指定が間違っています"

    def_exception :NoImpliment,
      "まだ出来ていません(%s)"

    def_exception :NoSupportRubyEncoding,
      "Ruby(%s)ではエンコーディングの指定はできません"

    def_exception :NoTmpDir,
      "fairy用のテンポラリディレクトリが存在しません(CONF.TMP_DIR=%s)"

    def_exception :NoLogDir,
      "fairy用のログディレクトリが存在しません(CONF.LOG_FILE=%s)"

    def_exception :CantExecSubcmd,
      "サブコマンドが実行できません(%s)"

    module INTERNAL
      extend Exception2MessageMapper

      def_exception :NoSuchDefiledUserLevelFilter, 
	"ユーザーレベルフィルタ(%s)は定義されていません"
      def_exception :CantDefExport, 
	"クラス以外を登録するときにはサービス名が必要です(%s)"
      def_exception :NoRegisterService,
	"サービス名が登録されていません(%s)"

      def_exception :UndefinedPolicy, "未サポートのポリシー(%s)"

      def_exception :UndefinedBackendClass, "Backend Classが定義されていません"
      def_exception :UndefinedNodeClass, "Node Classが定義されていません"

      def_exception :ShouldDefineSubclass, "サブクラスで定義してください"

      def_exception :ShouldNotSetInput, "インプットフィルタ(%s)にはinputを設定出来ません"

      def_exception :MarshalQueueNotEmpty, "Marshal queue は Emptyでなくてはなりません"
    end
  end

end
