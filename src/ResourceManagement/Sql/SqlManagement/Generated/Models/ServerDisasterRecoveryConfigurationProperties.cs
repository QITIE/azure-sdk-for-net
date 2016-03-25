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

namespace Microsoft.Azure.Management.Sql.Models
{
    /// <summary>
    /// Represents the properties of an Azure SQL Server disaster recovery
    /// configuration.
    /// </summary>
    public partial class ServerDisasterRecoveryConfigurationProperties
    {
        private string _autoFailover;
        
        /// <summary>
        /// Optional. Whether or not automatic failover is enabled.
        /// </summary>
        public string AutoFailover
        {
            get { return this._autoFailover; }
            set { this._autoFailover = value; }
        }
        
        private string _failoverPolicy;
        
        /// <summary>
        /// Optional. The policy for automatically failing over.
        /// </summary>
        public string FailoverPolicy
        {
            get { return this._failoverPolicy; }
            set { this._failoverPolicy = value; }
        }
        
        private string _partnerLogicalServerName;
        
        /// <summary>
        /// Optional. Gets the name of the partner server.
        /// </summary>
        public string PartnerLogicalServerName
        {
            get { return this._partnerLogicalServerName; }
            set { this._partnerLogicalServerName = value; }
        }
        
        private string _partnerServerId;
        
        /// <summary>
        /// Optional. Gets the id of the partner server.
        /// </summary>
        public string PartnerServerId
        {
            get { return this._partnerServerId; }
            set { this._partnerServerId = value; }
        }
        
        private string _role;
        
        /// <summary>
        /// Optional. Gets the role of the server.
        /// </summary>
        public string Role
        {
            get { return this._role; }
            set { this._role = value; }
        }
        
        private string _type;
        
        /// <summary>
        /// Optional. The type of server disaster recovery configuration.
        /// </summary>
        public string Type
        {
            get { return this._type; }
            set { this._type = value; }
        }
        
        /// <summary>
        /// Initializes a new instance of the
        /// ServerDisasterRecoveryConfigurationProperties class.
        /// </summary>
        public ServerDisasterRecoveryConfigurationProperties()
        {
        }
    }
}
