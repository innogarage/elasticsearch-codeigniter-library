<?php
class elasticsearch {

    public $conn;

    function __construct() {

        $ci =& get_instance();

        $ci->load->config('elasticsearch');

        $this->conn->server 	= $ci->config->item('es_server');
        $this->conn->port 		= $ci->config->item('es_port');
        $this->conn->username 	= $ci->config->item('es_username');
        $this->conn->password 	= $ci->config->item('es_password');

    }

    private function _request($path, $data, $method) {

        $ch = curl_init();
        $url = $this->conn->server . "/" . $path;

        $qry = json_encode($data);

        curl_setopt($ch, CURLOPT_URL, $url);
        curl_setopt($ch, CURLOPT_PORT, $this->conn->port);
        curl_setopt($ch, CURLOPT_CONNECTTIMEOUT, 1);
        curl_setopt($ch, CURLOPT_TIMEOUT, 3); //timeout in seconds
        curl_setopt($ch, CURLOPT_RETURNTRANSFER, 1);
        curl_setopt($ch, CURLOPT_CUSTOMREQUEST, strtoupper($method));
        curl_setopt($ch, CURLOPT_HTTPHEADER, array('X-HTTP-Method-Override: ' . strtoupper($method)));
        if (!empty($data))
            curl_setopt($ch, CURLOPT_POSTFIELDS, $qry);

        $result = curl_exec($ch);

        $ret = new stdClass();
        if($result === false) {

            $ret->error = 'Curl error: '. curl_error($ch);

            curl_close($ch);

            return $ret;

        } else {

            $ret = json_decode($result);
            $ret->_url = $url;

            curl_close($ch);

            return $ret;

        }

    }

    public function remove($path) {

        return $this->_request($path, array(), "DELETE");

    }

    public function search($path, $data) {

        return $this->_request($path . "/_search", $data, "POST");

    }

    public function msearch($path, $data) {

        return $this->_request($path . "/_msearch", $data, "POST");

    }

    public function update($path, $data, $version = 0) {

        $ver = $version > 0 ? "?version=".$version."&version_type=external" : "";

        return $this->_request($path . "/_update" . $ver, $data, "POST");

    }

    public function insert($path, $data, $version = 0) {

        $ver = $version > 0 ? "?version=".$version."&version_type=external" : "";

        return $this->_request($path . "/_create" . $ver, $data, "PUT");

    }

    public function count($path, $data = array()) {

        return $this->_request($path . "/_count", $data, "GET");

    }

    public function request($path, $data = array()) {

        return $this->_request($path, $data, "GET");

    }

}