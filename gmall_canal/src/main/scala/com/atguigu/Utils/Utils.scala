package com.atguigu.Utils

import java.util.ResourceBundle

object Utils {

    private val bundle: ResourceBundle = ResourceBundle.getBundle("config")
    def apply(propkey : String): String ={
      bundle.getString(propkey)
    }


}
