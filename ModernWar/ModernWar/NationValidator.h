#pragma once
#include <string>
#include <array>
#include <vector>

#include "NationState.h"


namespace ModernWarCore
{
	std::string ValidateNation(const NationState& nation);
	std::string ValidateRoster(const std::vector<NationState>& nations);
}