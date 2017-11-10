#include "pingcap_com_MagicProto.h"

JNIEXPORT jstring JNICALL Java_pingcap_com_MagicProto_version(JNIEnv * env, jobject obj)
{
    static const char *version = "v0.1";
    return env->NewStringUTF(version);
}
