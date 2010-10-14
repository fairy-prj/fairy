#include <ruby.h>

#define MULTIPLIER  137

static VALUE simple_hash(VALUE self, VALUE vstr);


static VALUE mFairy;
static VALUE mSimpleHash;


static VALUE simple_hash(VALUE self, VALUE vstr) {
    VALUE vh;
    char *str;
    unsigned int h = 0;

    str = StringValuePtr(vstr);

    for (; *str != '\0'; str++) {
        h = h * MULTIPLIER + *str;
    }

    vh = UINT2NUM(h);
    return vh;
}

void Init_simple_hash(void) {
    mFairy = rb_define_module("Fairy");
    mSimpleHash = rb_define_module_under(mFairy, "SimpleHash");
    
    rb_define_module_function(mSimpleHash, "hash", simple_hash, 1);
}


