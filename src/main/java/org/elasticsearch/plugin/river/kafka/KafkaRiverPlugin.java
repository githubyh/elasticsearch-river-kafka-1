/*
 * Copyright 2013 Mariam Hakobyan
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.elasticsearch.plugin.river.kafka;

import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.river.RiversModule;
import org.elasticsearch.river.kafka.KafkaRiverModule;


public class KafkaRiverPlugin extends AbstractPlugin {

    @Override
    public String name() {
        return "river-kafka";      // is the name of the plugin directory, which will appear in elasticsearch installation
    }

    @Override
    public String description() {
        return "River Kafka Plugin";
    }

    public void onModule(RiversModule module) {
        module.registerRiver("kafka", KafkaRiverModule.class);    // kafka is the type of river, which you must

        // give as the same value when creating river via command line
        //example

//        curl -XPUT 'localhost:9200/_river/my_kafka_river/_meta' -d '{
//        "type" : "kafka",
//                "kafka" : {
//                "brokerHost" : "localhost",
//                    "brokerPort" : 9092,
//                    "topic" : "dummy",
//                    "partition" : 0
//
//                }
//        }'
    }
}

