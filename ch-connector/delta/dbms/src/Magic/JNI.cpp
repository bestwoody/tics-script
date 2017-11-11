#include "pingcap_com_MagicProto.h"
#include "Sessions.h"

jstring bufferToJstring(JNIEnv * env, uint8_t * data, size_t cb)
{
    void * buf = malloc(cb);
    auto result = env->NewByteArray(cb);
    env->SetByteArrayRegion(result, 0, cb, (jbyte*)data);
    free(buf);
    return result;
}

JNIEXPORT jstring JNICALL Java_pingcap_com_MagicProto_version(JNIEnv * env, jobject obj)
{
    static const char *version = "v0.1";
    return env->NewStringUTF(version);
}

JNIEXPORT void JNICALL Java_pingcap_com_MagicProto_init(JNIEnv * env, jobject obj, jstring config)
{
    auto sessions = Magic::Sessions::global();
    sessions->init();
}

JNIEXPORT void JNICALL Java_pingcap_com_MagicProto_finish(JNIEnv * env, jobject obj)
{
    sessions->close();
}

JNIEXPORT jlong JNICALL Java_pingcap_com_MagicProto_query(JNIEnv * env, jobject obj, jstring query)
{
    auto query_cstr = env->GetStringUTFChars(query, false);
    if (!query_cstr)
        return -1;
    auto sessions = Magic::Sessions::global();
    auto token = sessions->newSession(query_cstr);
    env->ReleaseStringUTFChars(query_string, str);
    return token;
}

JNIEXPORT void JNICALL Java_pingcap_com_MagicProto_close(JNIEnv * env, jobject obj, jlong token)
{
    auto sessions = Magic::Sessions::global();
    if (session)
        sessions->closeSession(token);
}

JNIEXPORT jstring JNICALL Java_pingcap_com_MagicProto_error(JNIEnv * env, jobject obj, jlong token)
{
    jstring result = NULL;
    auto sessions = Magic::Sessions::global();
    if (!session)
        return result;
    auto & error = sessions->getErrorString(token);
    if (error.empty())
        return result;
    return env->NewStringUTF(error.c_str());
}

JNIEXPORT jbyteArray JNICALL Java_pingcap_com_MagicProto_next(JNIEnv * env, jobject obj, jlong token)
{
    auto sessions = Magic::Sessions::global();
    auto session = sessions->session(token);
    jbyteArray result = NULL;
    if (!session)
        return result;
    auto buf = session->getEncodedBlock();
    if (!buf)
        return result;
    return bufferToJstring(env, (uint8_t*)buf->data(), buf->size())
}

JNIEXPORT jbyteArray JNICALL Java_pingcap_com_MagicProto_schema(JNIEnv * env, jobject obj, jlong token)
{
    auto sessions = Magic::Sessions::global();
    auto session = sessions->session(token);
    jbyteArray result = NULL;
    if (!session)
        return result;
    auto buf = session->getEncodedSchema();
    if (!buf)
        return result;
    return bufferToJstring(env, (uint8_t*)buf->data(), buf->size())
}
