#include "pingcap_com_MagicProto.h"
#include "Sessions.h"
#include "Context.h"


// TODO: memory leak check

namespace JNIHelper
{
    jbyteArray bufferToJByteArray(JNIEnv * env, uint8_t * data, size_t cb)
    {
        void * buf = malloc(cb);
        auto result = env->NewByteArray(cb);
        env->SetByteArrayRegion(result, 0, cb, (jbyte*)data);
        free(buf);
        return result;
    }
}


JNIEXPORT jstring JNICALL Java_pingcap_com_MagicProto_version(JNIEnv * env, jobject obj)
{
    static const char *version = "v0.1";
    return env->NewStringUTF(version);
}


JNIEXPORT jobject JNICALL Java_pingcap_com_MagicProto_init(JNIEnv * env, jobject obj, jstring config)
{
    jclass resultNull = NULL;
    jclass result = env->FindClass("pingcap/com/MagicProto$InitResult");
    if (!result)
        return resultNull;
    jfieldID errorId = env->GetFieldID(result, "error", "Ljava/lang/String;");
    if (!errorId)
        return resultNull;

    auto sessions = Magic::Sessions::global();
    auto config_cstr = env->GetStringUTFChars(config, 0);
    auto error = sessions->init(config_cstr);
    env->ReleaseStringUTFChars(config, config_cstr);

    if (!error.empty())
        env->SetObjectField(result, errorId, env->NewStringUTF(error.c_str()));
    return result;
}


JNIEXPORT jobject JNICALL Java_pingcap_com_MagicProto_finish(JNIEnv * env, jobject obj)
{
    jclass resultNull = NULL;
    jclass result = env->FindClass("pingcap/com/MagicProto$FinishResult");
    if (!result)
        return resultNull;
    jfieldID errorId = env->GetFieldID(result, "error", "Ljava/lang/String;");
    if (!errorId)
        return resultNull;

    auto sessions = Magic::Sessions::global();
    auto error = sessions->close();

    if (!error.empty())
        env->SetObjectField(result, errorId, env->NewStringUTF(error.c_str()));
    return result;
}


JNIEXPORT jobject JNICALL Java_pingcap_com_MagicProto_query(JNIEnv * env, jobject obj, jstring query)
{
    jclass resultNull = NULL;
    jclass result = env->FindClass("pingcap/com/MagicProto$QueryResult");
    if (!result)
        return resultNull;
    jfieldID errorId = env->GetFieldID(result, "error", "Ljava/lang/String;");
    if (!errorId)
        return resultNull;
    jfieldID tokenId = env->GetFieldID(result, "token", "J");
    if (!tokenId)
        return resultNull;

    std::string error;
    jlong token = -1;
    auto query_cstr = env->GetStringUTFChars(query, 0);
    if (query_cstr)
    {
        auto sessions = Magic::Sessions::global();
        auto session = sessions->newSession(query_cstr);
        env->ReleaseStringUTFChars(query, query_cstr);
        token = session.token;
        error = session.error;
    } else {
        error = "env->GetStringUTFChars() failed";
    }

    if (!error.empty())
        env->SetObjectField(result, errorId, env->NewStringUTF(error.c_str()));
    env->SetLongField(result, tokenId, token);
    return result;
}


// TODO: no impl
JNIEXPORT jobject JNICALL Java_pingcap_com_MagicProto_querys(JNIEnv * env, jobject obj)
{
    jclass resultNull = NULL;
    jclass result = env->FindClass("pingcap/com/MagicProto$QuerysResult");
    if (!result)
        return resultNull;
    jfieldID errorId = env->GetFieldID(result, "error", "Ljava/lang/String;");
    if (!errorId)
        return resultNull;
    //jfieldID tokensId = env->GetFieldID(result, "tokens", "TODO");
    //if (!tokenId)
    //    return resultNull;

    //auto sessions = Magic::Sessions::global();
    //auto session = sessions->getSession(token);
    //if (!session)
    //    return resultNull;

    //std::vector<long> tokens;
    //sessions.getTokens(tokens);
    //jlongArray array = env->NewLongArray(tokens.size());
    //env->SetLongArrayRegion(array, 0, tokens.size(), (jlong*)&tokens[0]);
    //env->SetLongField(result, tokensId, array);
    std::string error = "TODO: no impl";
    env->SetObjectField(result, errorId, env->NewStringUTF(error.c_str()));
    return result;
}


JNIEXPORT void JNICALL Java_pingcap_com_MagicProto_close(JNIEnv * env, jobject obj, jlong token)
{
    auto sessions = Magic::Sessions::global();
    auto session = sessions->getSession(token);
    if (session)
        sessions->closeSession(token);
}


JNIEXPORT jstring JNICALL Java_pingcap_com_MagicProto_error(JNIEnv * env, jobject obj, jlong token)
{
    auto sessions = Magic::Sessions::global();
    auto session = sessions->getSession(token);
    jstring result = NULL;
    if (!session)
        return result;
    auto & error = session->getErrorString();
    if (error.empty())
        return result;
    return env->NewStringUTF(error.c_str());
}


JNIEXPORT jbyteArray JNICALL Java_pingcap_com_MagicProto_next(JNIEnv * env, jobject obj, jlong token)
{
    auto sessions = Magic::Sessions::global();
    auto session = sessions->getSession(token);
    jbyteArray result = NULL;
    if (!session)
        return result;
    auto buf = session->getEncodedBlock();
    if (!buf)
        return result;
    return JNIHelper::bufferToJByteArray(env, (uint8_t*)buf->data(), buf->size());
}


JNIEXPORT jbyteArray JNICALL Java_pingcap_com_MagicProto_schema(JNIEnv * env, jobject obj, jlong token)
{
    auto sessions = Magic::Sessions::global();
    auto session = sessions->getSession(token);
    jbyteArray result = NULL;
    if (!session)
        return result;
    auto buf = session->getEncodedSchema();
    if (!buf)
        return result;
    return JNIHelper::bufferToJByteArray(env, (uint8_t*)buf->data(), buf->size());
}
