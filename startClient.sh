#!/bin/bash
java -client -Djava.net.preferIPv4Stack=true -cp ".;lib/*;classes/" gash.router.app.DemoApp localhost 4568