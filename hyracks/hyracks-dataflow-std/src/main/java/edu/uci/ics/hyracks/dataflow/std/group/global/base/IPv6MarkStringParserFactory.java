/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.hyracks.dataflow.std.group.global.base;

import java.io.DataOutput;
import java.io.IOException;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.IValueParser;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.IValueParserFactory;

public class IPv6MarkStringParserFactory implements IValueParserFactory {

    private static final IValueParserFactory[] INSTANCES = new IValueParserFactory[33];

    private static final long serialVersionUID = 1L;

    private boolean flipBit;

    private int mask;

    private IPv6MarkStringParserFactory(
            int mask) {
        this.mask = mask;
        this.flipBit = false;
    }

    private IPv6MarkStringParserFactory(
            int mask,
            boolean flipBit) {
        this.mask = mask;
        this.flipBit = flipBit;
    }

    public static IValueParserFactory getInstance(
            int mask) {
        if (INSTANCES[0] == null) {
            for (int i = 0; i < 33; i++) {
                INSTANCES[i] = new IPv6MarkStringParserFactory(i);
            }
        }
        return INSTANCES[mask];
    }

    public static IValueParserFactory getInstance(
            int mask,
            boolean flipBit) {
        if (INSTANCES[0] == null) {
            for (int i = 0; i < 33; i++) {
                INSTANCES[i] = new IPv6MarkStringParserFactory(i, flipBit);
            }
        }
        return INSTANCES[mask];
    }

    /*
     * (non-Javadoc)
     * 
     * @see edu.uci.ics.hyracks.dataflow.common.data.parsers.IValueParserFactory#createValueParser()
     */
    @Override
    public IValueParser createValueParser() {
        return new IValueParser() {
            private byte[] ip;

            private boolean fliper = true;
            
            @Override
            public void parse(
                    char[] buffer,
                    int start,
                    int length,
                    DataOutput out) throws HyracksDataException {

                ip = new byte[length + 2];

                int count = 0;
                ip[count++] = (byte) ((length >>> 8) & 0xff);
                ip[count++] = (byte) ((length >>> 0) & 0xff);

                int toReplace = mask;

                for (int i = 0; i < length; i++) {
                    char ch = buffer[start + i];
                    if (!((ch >= '0' && ch <= '9') || (ch >= 'A' && ch <= 'F') || (ch >= 'a' && ch <= 'f') || (ch == ':'))) {
                        throw new HyracksDataException("Containing invalid character (" + ch + ") for a IPv6 string");
                    }
                    if (ch != ':' && toReplace > 0) {
                        if (flipBit) {
                            if (fliper) {
                                ip[count++] = '0';
                            } else {
                                ip[count++] = '1';
                            }
                        } else {
                            ip[count++] = 'f';
                        }
                        toReplace--;
                    } else {
                        ip[count++] = (byte) ch;
                    }
                }
                
                fliper = !fliper;

                try {
                    out.write(ip, 0, ip.length);
                } catch (IOException e) {
                    throw new HyracksDataException(e);
                }
            }
        };
    }

}
