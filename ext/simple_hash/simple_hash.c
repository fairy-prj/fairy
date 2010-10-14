#include <ruby.h>

#define MULTIPLIER  137

static VALUE util_simple_hash(VALUE self, VALUE vstr);


static VALUE mFairy;
static VALUE mExt;


static VALUE util_simple_hash(VALUE self, VALUE vstr) {
    VALUE vh;
    char *str;
    unsigned int h = 0;

    str = RSTRING_PTR(vstr);

    for (; *str != '\0'; str++) {
        h = h * MULTIPLIER + *str;
    }

    vh = UINT2NUM(h);
    return vh;
}

void Init_util_ext(void) {
    mFairy = rb_define_module("Fairy");
    mExt = rb_define_module_under(mFairy, "Ext");
    
    rb_define_module_function(mExt, "simple_hash", util_simple_hash, 1);
}


