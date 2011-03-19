/**********************************************************************

  fairy.c -
  Copyright (C) 2007-2011 Rakuten, Inc.

**********************************************************************/

#include "ruby.h"
#include "fairy.h"

extern void Init_simple_hash();
extern void Init_string_buffer();
extern void Init_xmarshaled_queue();

VALUE rb_mFairy;
VALUE rb_cFairyLog;
VALUE rb_cFairyImport;

VALUE rb_FairyConf;

static ID id_aref;

VALUE
rb_fairy_conf(char *conf_attr, VALUE policy, char *policy_name)
{
  VALUE val = Qnil;
  
  if (policy) {
    val = rb_funcall(policy, id_aref, 1, ID2SYM(rb_intern(policy_name)));
  }
  
  if (NIL_P(val)) {
    val = rb_funcall(rb_FairyConf, rb_intern(conf_attr), 0);
  }
  return val;
}

#define DEF_LOG_FUNC(LEVEL) \
static ID id_##LEVEL; \
VALUE \
rb_fairy_##LEVEL(VALUE sender, char *message) \
{ \
  return rb_funcall(rb_cFairyLog, id_##LEVEL, 2, \
		    sender, rb_str_new_cstr(message));	\
} \
static ID id_##LEVEL##_exception; \
VALUE \
rb_fairy_##LEVEL##_exception(VALUE sender) \
{ \
  return rb_funcall(rb_cFairyLog, id_##LEVEL##_exception, 1, \
		    sender); \
} \
static ID id_##LEVEL##_backtrace; \
VALUE \
rb_fairy_##LEVEL##_backtrace(VALUE sender) \
{ \
  return rb_funcall(rb_cFairyLog, id_##LEVEL##_backtrace, 1, sender);	\
}

DEF_LOG_FUNC(fatal);
DEF_LOG_FUNC(error);
DEF_LOG_FUNC(warn);
DEF_LOG_FUNC(info);
DEF_LOG_FUNC(verbose);
DEF_LOG_FUNC(debug);

#define LOG_PRE ""
#define LOG_POST "_exception"

#define DEF_LOG_ID(level) \
  id_##level = rb_intern(#level); \
  id_##level##_exception = rb_intern(LOG_PRE #level LOG_POST); \
  id_##level##_backtrace = rb_intern("" #level "_backtrace");

static ID id_debug_p;

VALUE
rb_fairy_debug_p(VALUE obj)
{
  return rb_funcall(rb_cFairyLog, id_debug_p, 1, obj);
}

Init_fairy()
{
  rb_mFairy = rb_define_module("Fairy");

  rb_require("fairy/share/port");

  rb_cFairyLog = rb_const_get(rb_mFairy, rb_intern("Log"));
  rb_cFairyImport = rb_const_get(rb_mFairy, rb_intern("Import"));
  
  rb_FairyEOS = rb_intern("END_OF_STREAM");
  rb_FairyConf = rb_const_get(rb_mFairy, rb_intern("CONF"));

  id_aref = rb_intern("[]");
  
  DEF_LOG_ID(fatal);
  DEF_LOG_ID(error);
  DEF_LOG_ID(warn);
  DEF_LOG_ID(info);
  DEF_LOG_ID(verbose);
  DEF_LOG_ID(debug);
  
  id_debug_p = rb_intern("debug_p");

  Init_simple_hash();
  Init_string_buffer();
  Init_xmarshaled_queue();

  rb_fairy_warn(rb_mFairy, "fairy.so initialize OK");
}
