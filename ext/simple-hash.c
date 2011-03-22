/*
 * Copyright (C) 2007-2010 Rakuten, Inc.
 */

#include <ruby.h>

#include "fairy.h"

#define MULTIPLIER  137

VALUE rb_mFairySimpleHash;

VALUE
rb_fairy_simple_hash(VALUE self, VALUE vstr)
{
  return INT2FIX(rb_fairy_simple_hash_uint(self, vstr));
}

inline unsigned int
rb_fairy_simple_hash_uint(VALUE self, VALUE vstr)
{
  VALUE vh;
  char *str;
  int len;
  char *p;
  unsigned int h = 0;

  str = StringValuePtr(vstr);
  len = RSTRING_LEN(vstr);

  for (p = str; p - str < len; p++) {
    h = h * MULTIPLIER + *p;
  }
  return h;
}


void Init_simple_hash(void) {
  rb_mFairySimpleHash = rb_define_module_under(rb_mFairy, "SimpleHash");
    
  rb_define_module_function(rb_mFairySimpleHash, "hash", rb_fairy_simple_hash, 1);
}


