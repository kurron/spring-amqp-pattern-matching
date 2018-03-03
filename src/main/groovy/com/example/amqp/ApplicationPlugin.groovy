package com.example.amqp

import com.amazonaws.xray.plugins.Plugin

/**
 * Provides X-Ray with information about the application.
 */
class ApplicationPlugin implements Plugin {

    @Override
    String getOrigin() {
        'My Origin'
    }

    @Override
    String getServiceName() {
        'My Service Name'
    }

    @Override
    Map<String, Object> getRuntimeContext() {
        ['a': '1', 'b': '2']
    }
}
