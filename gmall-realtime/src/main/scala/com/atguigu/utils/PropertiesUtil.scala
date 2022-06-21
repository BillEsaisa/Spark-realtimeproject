package com.atguigu.utils

import java.io.InputStreamReader
import java.util.{Properties, ResourceBundle}

object PropertiesUtil {
  private val bundle: ResourceBundle = ResourceBundle.getBundle("config")
 def apply(propkey : String): String ={
   bundle.getString(propkey)
 }
}

