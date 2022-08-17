package com.kubeforce.awsiots3;

import java.util.Map;
import java.util.function.Consumer;

public class IoTConsumer implements Consumer<Map<String,String>> {
    @Override
    public void accept (Map<String, String> map)
    {

        MqttSubscriber mqttSubscriber = new MqttSubscriber();
        S3Upload s3Upload = new S3Upload();
        s3Upload.S3upload(mqttSubscriber.MqttSubscribe());

    }
}
