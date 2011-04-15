/**********************************************************************

  fairy.c -
  Copyright (C) 2007-2011 Rakuten, Inc.

**********************************************************************/

#include <stdarg.h>

#include "ruby.h"
#include "fairy.h"

extern void Init_simple_hash();
extern void Init_fixnum_buffer();
extern void Init_string_buffer();
extern void Init_sized_queue();
extern void Init_xmarshaled_queue();
extern void Init_p_group_by();
extern void Init_p_xgroup_by();

VALUE rb_mFairy;
VALUE rb_cFairyLog;
VALUE rb_cFairyProcessor = Qnil;

VALUE rb_cFairyImport;
VALUE rb_cFairyExport;

VALUE rb_cFairyImportCTLTOKEN_SET_NO_IMPORT;
VALUE rb_cFairyImportCTLTOKEN_NULLVALUE;
VALUE rb_cFairyImportCTLTOKEN_DELAYED_ELEMENT;

VALUE rb_FairyConf;
VALUE rb_FairyEOS;

static ID id_aref;
static ID id_def_export;

VALUE
rb_fairy_conf(const char *conf_attr, VALUE policy, const char *policy_name)
{
  VALUE val = Qnil;

  if (policy) {
    val = rb_funcall(policy, id_aref, 1, ID2SYM(rb_intern(policy_name)));
  }
  
  if (NIL_P(val) && conf_attr != NULL) {
    val = rb_funcall(rb_FairyConf, rb_intern(conf_attr), 0);
  }
  return val;
}

VALUE
rb_fairy_processor_def_export(VALUE klass)
{
  return rb_funcall(rb_cFairyProcessor, id_def_export, 1, klass);
}

VALUE
rb_fairy_processor_def_export2(VALUE klass, char *name)
{
  VALUE str = rb_str_new2(name);
  
  return rb_funcall(rb_cFairyProcessor, id_def_export, 2, klass, str);
}

#define DEF_LOG_FUNC(LEVEL) \
  static ID id_##LEVEL;	    \
  VALUE						    \
  rb_fairy_##LEVEL(VALUE sender, const char *message)	\
  {							\
    return rb_funcall(rb_cFairyLog, id_##LEVEL, 2,	\
		      sender, rb_str_new_cstr(message));	\
  }								\
  VALUE								\
  rb_fairy_##LEVEL##f(VALUE sender, const char *format, ...)	\
  {									\
    VALUE result;						\
    va_list ap;								\
    va_start(ap, format);						\
    result = rb_vsprintf(format, ap);					\
    va_end(ap);								\
    return rb_funcall(rb_cFairyLog, id_##LEVEL, 2, sender, result);	\
  }									\
  static ID id_##LEVEL##_exception;					\
  VALUE									\
  rb_fairy_##LEVEL##_exception(VALUE sender)				\
  {									\
    return rb_funcall(rb_cFairyLog, id_##LEVEL##_exception, 1,		\
		      sender);						\
  }									\
  static ID id_##LEVEL##_backtrace;					\
  VALUE									\
  rb_fairy_##LEVEL##_backtrace(VALUE sender)				\
  {									\
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

VALUE
rb_fairy_debug_p2(VALUE self, const char *pre, VALUE obj)
{
  const char post[] = ": %s";
  char *buf = ALLOCA_N(char, strlen(pre) + strlen(post) + 1);
  
  strcpy(buf, pre);
  strcat(buf, post);
    
  return rb_fairy_debugf(self, buf, RSTRING_PTR(rb_inspect(obj)));
}

Init_fairy()
{
  rb_mFairy = rb_define_module("Fairy");
  rb_FairyConf = rb_const_get(rb_mFairy, rb_intern("CONF"));

  if (rb_const_defined(rb_mFairy, rb_intern("Processor"))) {
      rb_cFairyProcessor = rb_const_get(rb_mFairy, rb_intern("Processor"));
  }

  rb_require("fairy/share/port");

  rb_cFairyLog = rb_const_get(rb_mFairy, rb_intern("Log"));

  rb_cFairyImport = rb_const_get(rb_mFairy, rb_intern("Import"));
  rb_cFairyExport = rb_const_get(rb_mFairy, rb_intern("Export"));
  
  rb_cFairyImportCTLTOKEN_SET_NO_IMPORT =
    rb_const_get(rb_cFairyImport, rb_intern("CTLTOKEN_SET_NO_IMPORT"));
  rb_cFairyImportCTLTOKEN_NULLVALUE =
    rb_const_get(rb_cFairyImport, rb_intern("CTLTOKEN_NULLVALUE"));
  rb_cFairyImportCTLTOKEN_DELAYED_ELEMENT =
    rb_const_get(rb_cFairyImport, rb_intern("CTLTOKEN_DELAYED_ELEMENT"));
		 
  rb_FairyEOS = ID2SYM(rb_intern("END_OF_STREAM"));

  id_aref = rb_intern("[]");
  id_def_export = rb_intern("def_export");
  
  DEF_LOG_ID(fatal);
  DEF_LOG_ID(error);
  DEF_LOG_ID(warn);
  DEF_LOG_ID(info);
  DEF_LOG_ID(verbose);
  DEF_LOG_ID(debug);
  
  id_debug_p = rb_intern("debug_p");

  Init_simple_hash();
  Init_fixnum_buffer();
  Init_string_buffer();
  Init_xsized_queue();
  Init_xmarshaled_queue();
  Init_p_group_by();
  Init_p_xgroup_by();

  rb_fairy_warn(rb_mFairy, "fairy.so initialize OK");
}
