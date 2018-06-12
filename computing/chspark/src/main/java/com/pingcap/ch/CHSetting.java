package com.pingcap.ch;

import com.google.common.base.Preconditions;

import com.pingcap.common.WriteBuffer;

import java.io.IOException;
import java.util.List;

public abstract class CHSetting {
    public String name;

    public CHSetting(String name) {
        this.name = name;
    }

    public abstract void write(WriteBuffer writer) throws IOException;

    public static void serialize(List<CHSetting> settings, WriteBuffer writer) throws IOException {
        for (CHSetting setting : settings) {
            writer.writeUTF8StrWithVarLen(setting.name);
            setting.write(writer);
        }
        // An empty string is a marker for the end of the settings.
        writer.writeUTF8StrWithVarLen("");
    }

    public static class SettingUInt extends CHSetting {
        public long value;

        public SettingUInt(String name, long value) {
            super(name);
            Preconditions.checkArgument(value >= 0);
            this.value = value;
        }

        @Override
        public void write(WriteBuffer writer) throws IOException {
            writer.writeVarUInt64(value);
        }
    }

    public static class SettingString extends CHSetting {
        public String value;

        public SettingString(String name, String value) {
            super(name);
            this.value = value;
        }

        @Override
        public void write(WriteBuffer writer) throws IOException {
            writer.writeUTF8StrWithVarLen(value);
        }
    }

}
