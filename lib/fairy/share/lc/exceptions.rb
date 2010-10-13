# encoding: UTF-8
#
# Copyright (C) 2007-2010 Rakuten, Inc.
#

require "e2mmap"

module Fairy

  class BreakCreateNode<Exception;end
  class AbortCreateNode<Exception;end

  module ERR
    extend Exception2MessageMapper

    def_exception :CantAcceptBlock,
      "Can't accept block"
    def_exception :NodeNotArrived, 
      "Node don't arrived host(%s)"
    def_exception :NoExistProcessorWithObject, 
      "No Exists Processor within object(%s)"

    def_exception :NoSuchMode, 
      "No such mode(%s)"
    def_exception :UnrecognizedOption,
      "Unrecognized option(%s)"

    def_exception :AlreadyAssignedVarriable,
      "Already assigned varriable(%s)"

    def_exception :NoAssignedVarriable,
      "No assigned varriable(%s)"

    def_exception :NoSupportClass,
      "No support class(%s)"

    def_exception :NoVFile,
      "Not a vfile(%s)"

    def_exception :IllegalVFile,
      "Illegal vfile"

    def_exception :NoImpliment,
      "Yet, no impliment(%s)"

    def_exception :NoSupportRubyEncoding,
      "Ruby(%s) isn't support Encoding"

    def_exception :NoTmpDir,
      "No exists fairy tmp dir(CONF.TMP_DIR=%s)"

    def_exception :NoLogDir,
      "No exists fairy log dir(CONF.LOG_FILE=%s)"

    def_exception :CantExecSubcmd,
      "Can't execute subcommand(%s)"

    module INTERNAL
      extend Exception2MessageMapper

      def_exception :NoSuchDefiledUserLevelFilter, 
	"No such defined user level filter(%s)"

      def_exception :CantDefExport, 
	"Should have Service name except class(%s)"
      def_exception :NoRegisterService,
	"No register service(%s)"

      def_exception :UndefinedPolicy, "Undefined policy(%s)"

      def_exception :UndefinedBackendClass, "Undefined Backend Class"
      def_exception :UndefinedNodeClass, "Undefined Node Class"

      def_exception :ShouldDefineSubclass, "Should define subclass"

      def_exception :ShouldNotSetInput, "Should not set input(%s)"

      def_exception :MarshalQueueNotEmpty, "Marshal queue no empty"
    end
  end

end
