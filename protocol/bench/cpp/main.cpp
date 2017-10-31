#include <iostream>
#include "pingcap_com_MagicProtoBench.h"

JNIEXPORT jint JNICALL Java_pingcap_com_MagicProtoBench_sumInt(JNIEnv *env, jobject obj, jint a, jint b) {
    return a + b;
}

JNIEXPORT jdouble JNICALL Java_pingcap_com_MagicProtoBench_sumDouble(JNIEnv *env, jobject obj, jdouble a, jdouble b) {
    return a + b;
}

JNIEXPORT jbyteArray JNICALL Java_pingcap_com_MagicProtoBench_alloc(JNIEnv *env, jobject obj, jint size) {
    void *buf = malloc(size);
    jbyteArray result = env->NewByteArray(size);
    env->SetByteArrayRegion(result, 0, size, (jbyte*)buf);
    free(buf);
    return result;
}

int main() {
    jdouble result = 0;
    for (long i = 0; i < 1000000000; i++)
        result = Java_pingcap_com_MagicProtoBench_sumDouble(0, 0, 1, result);
    std::cout << result << std::endl;
}
