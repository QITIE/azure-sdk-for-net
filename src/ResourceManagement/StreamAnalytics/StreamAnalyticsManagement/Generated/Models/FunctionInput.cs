// 
// Copyright (c) Microsoft and contributors.  All rights reserved.
// 
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// 
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// 
// See the License for the specific language governing permissions and
// limitations under the License.
// 

// Warning: This code was generated by a tool.
// 
// Changes to this file may cause incorrect behavior and will be lost if the
// code is regenerated.

using System;
using System.Linq;

namespace Microsoft.Azure.Management.StreamAnalytics.Models
{
    /// <summary>
    /// The input parameter of the function.
    /// </summary>
    public partial class FunctionInput
    {
        private string _dataType;
        
        /// <summary>
        /// Optional. Gets or sets the data type of the function input
        /// parameter. These are Azure Stream Analytics data types
        /// (https://msdn.microsoft.com/en-us/library/azure/dn835065.aspx).
        /// </summary>
        public string DataType
        {
            get { return this._dataType; }
            set { this._dataType = value; }
        }
        
        private bool? _isConfigurationParameter;
        
        /// <summary>
        /// Optional. Gets or sets the isConfigurationParameter flag. True if
        /// this input parameter is expected to be a constant. Default is
        /// false.
        /// </summary>
        public bool? IsConfigurationParameter
        {
            get { return this._isConfigurationParameter; }
            set { this._isConfigurationParameter = value; }
        }
        
        /// <summary>
        /// Initializes a new instance of the FunctionInput class.
        /// </summary>
        public FunctionInput()
        {
        }
    }
}
