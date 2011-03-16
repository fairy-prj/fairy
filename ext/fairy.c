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
VALUE rb_cFairyImport;

Init_fairy()
{
  rb_mFairy = rb_define_module("Fairy");

  rb_require("fairy/share/port");

  rb_cFairyImport = rb_const_get(rb_mFairy, rb_intern("Import"));
  
  rb_SYM_EOS = rb_intern("END_OF_STREAM");

  Init_simple_hash();
  Init_string_buffer();
  Init_xmarshaled_queue();
}
