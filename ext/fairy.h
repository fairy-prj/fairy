/**********************************************************************

  fairy.h -
  Copyright (C) 2007-2011 Rakuten, Inc.

**********************************************************************/

RUBY_EXTERN VALUE rb_mFairy;
RUBY_EXTERN VALUE rb_cFairyImport;

RUBY_EXTERN VALUE rb_mFairySimpleHash;
RUBY_EXTERN VALUE rb_cFairyStringBuffer;
RUBY_EXTERN VALUE rb_cFairyXMarshaledQueue;

RUBY_EXTERN VALUE rb_FairyConf;

RUBY_EXTERN VALUE rb_fairy_simple_hash(VALUE, VALUE);

RUBY_EXTERN VALUE rb_fairy_string_buffer_new(void);
RUBY_EXTERN VALUE rb_fairy_string_buffer_size(VALUE);
RUBY_EXTERN VALUE rb_fairy_string_buffer_push(VALUE, VALUE);
RUBY_EXTERN VALUE rb_fairy_string_buffer_to_a(VALUE);
RUBY_EXTERN VALUE rb_fairy_string_buffer_marshal_dump(VALUE);
RUBY_EXTERN VALUE rb_fairy_string_buffer_marshal_load(VALUE, VALUE);

ID rb_FairyEOS;

#define EOS_P(v) (SYMBOL_P(v) && (SYM2ID(v) == rb_FairyEOS))

RUBY_EXTERN VALUE rb_fairy_xmarshaled_queue_new(VALUE, VALUE, VALUE);
RUBY_EXTERN VALUE rb_fairy_xmarshaled_queue_push(VALUE, VALUE);
RUBY_EXTERN VALUE rb_fairy_xmarshaled_queue_pop(VALUE);
RUBY_EXTERN VALUE rb_fairy_xmarshaled_queue_inspect(VALUE);

RUBY_EXTERN VALUE rb_fairy_conf(char *, VALUE, char *);

#define DEF_LOG_FUNC_EXTERN(LEVEL) \
  RUBY_EXTERN VALUE rb_fairy_##LEVEL(VALUE, char *); \
  RUBY_EXTERN VALUE rb_fairy_##LEVEL##_exception(VALUE); \
  RUBY_EXTERN VALUE rb_fairy_##LEVEL##_backtrace(VALUE);

DEF_LOG_FUNC_EXTERN(fatal);
DEF_LOG_FUNC_EXTERN(error);
DEF_LOG_FUNC_EXTERN(warn);
DEF_LOG_FUNC_EXTERN(info);
DEF_LOG_FUNC_EXTERN(verbose);
DEF_LOG_FUNC_EXTERN(debug);












