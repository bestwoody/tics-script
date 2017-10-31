#include "pingcap_com_MagicProtoBench.h"


JNIEXPORT jdouble JNICALL Java_pingcap_com_MagicProtoBench_sum(JNIEnv *env, jobject obj, jdouble a, jdouble b) {
    return a + b;
}
